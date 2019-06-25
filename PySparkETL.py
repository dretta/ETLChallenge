import os
import sys
import csv
import inspect
from datetime import datetime
import psycopg2
from psycopg2 import sql
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType as iType
from pyspark.sql.types import StringType as sType
from pyspark.sql.types import DateType as dType
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType


class PsqlObject:
    def __init__(self):
        self.userName = 'postgres'
        self.password = 'postgres'
        self.hostName = 'localhost'
        self.database = 'campaign_finance'
        self.url = 'jdbc:postgresql://localhost/{}'.format(self.database)
        self.conn = psycopg2.connect("user={0} password={1} host={2} dbname={3}"
                                     .format(self.userName, self.password, self.hostName, self.database))
        self.conn.rollback()
        self.cur = self.conn.cursor()

    @staticmethod
    def checkForJar():
        jarDir = os.path.join(os.environ['SPARK_HOME'], 'jars')
        for fileName in os.listdir(jarDir):
            if fileName.startswith('postgresql'):
                return
        assert False, "No Postgresql jar found, Download it at \
                      https://jdbc.postgresql.org/download.html and place in %SPARK_HOME%\\jars"

    def printRow(self):
        print(' '.join("({0[0]},{0[1]})".format(r) for r in self.cur.fetchall()))

    def printHeaders(self):
        self.cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
        for table in self.cur.fetchall():
            tableName = table[0]
            print(tableName)
            query = """SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s"""
            arguments = (tableName,)
            self.cur.execute(query, arguments)
            self.printRow()
        self.printRow()

    def writeTable(self, tableName, tableDF):
        tableDF.write.jdbc(url=self.url, table=tableName, mode='overwrite',
                           properties={'user': self.userName, 'password': self.password})


class DataObject:
    def __init__(self, slices, debug):
        self.debug = debug
        self.dataFrames = {}
        self.sourceData = {
            'candidates': [
                ['cid', 'party', 'cycle'],
                [sType, sType,   iType]
            ],
            'committees': [
                ['committee_id', 'pac_short', 'party'],
                [sType,          sType,       sType]
            ],
            'industry_codes': [
                ['id',  'category_code', 'industry_code', 'sector'],
                [iType, sType,           sType,           sType]],
            'individual_contributions': [
                ['contributor_id', 'cycle',    'recipient_id', 'date', 'amount', 'real_code',
                 'committee_id',   'other_id', 'gender',       'city', 'state',  'zip'],
                [sType,            iType,      sType,           dType, iType,    sType,
                 sType,            sType,      sType,           sType, sType,    sType]
            ]
        }

        self.reportColumns = [
            ["TotalAmountDonated", self.totalContributed, None, None],
            ["TotalContributorSize", self.totalContributors, None, None],
            ["AverageAmountDonated", self.averageContribution, None, None],
            ["TotalFemaleAmountDonated", self.totalContributed, self.getGenderContributors, 'F'],
            ["TotalFemaleContributorSize", self.totalContributors, self.getGenderContributors, 'F'],
            ["AverageFemaleAmountDonated", self.averageContribution, self.getGenderContributors, 'F'],
            ["TotalMaleAmountDonated", self.totalContributed, self.getGenderContributors, 'M'],
            ["TotalMaleContributorSize", self.totalContributors, self.getGenderContributors, 'M'],
            ["AverageMaleAmountDonated", self.averageContribution, self.getGenderContributors, 'M'],
            ["TotalFirstTimeAmountDonated", self.totalContributed, self.getFirstCycleContributions, None],
            ["TotalFirstTimeContributorSize", self.totalContributors, self.getFirstCycleContributions, None],
            ["AverageFirstTimeAmountDonated", self.averageContribution, self.getFirstCycleContributions, None],
            ["TotalSmallAmountDonated", self.totalContributed, self.getContributionsByCycleBySize, False],
            ["TotalSmallContributorSize", self.totalContributors, self.getContributionsByCycleBySize, False],
            ["AverageSmallAmountDonated", self.averageContribution, self.getContributionsByCycleBySize, False],
            ["TotalLargeAmountDonated", self.totalContributed, self.getContributionsByCycleBySize, True],
            ["TotalLargeContributorSize", self.totalContributors, self.getContributionsByCycleBySize, True],
            ["AverageLargeAmountDonated", self.averageContribution, self.getContributionsByCycleBySize, True]
        ]

        self.tempDir = os.path.join(os.getcwd(), 'tmp')
        self.slices = slices
        self.contributionsByCycle = None

    def __enter__(self):
        # TODO: Add input that controls keeping/deleting temp directory
        # if os.path.exists(self.tempDir):
        #     self.removeTempDirectory()
        # os.mkdir(self.tempDir)
        if not os.path.exists(self.tempDir):
            os.mkdir(self.tempDir)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # self.removeTempDirectory()
        pass

    def removeTempDirectory(self):
        for f in os.listdir(self.tempDir):
            os.remove(os.path.join(self.tempDir, f))
        os.removedirs(self.tempDir)

    def createDataFramesFromPsql(self, sqlCtx, psqlObj):

        def convertItems(line):
            items = []
            for i in range(len(line)):
                itemType = columnTypes[i]
                if itemType == iType:
                    items.append(int(line[i]))
                elif itemType == dType:
                    try:
                        items.append(datetime.strptime(line[i], '%Y-%m-%d %H:%M:%S'))
                    except ValueError:
                        items.append(datetime.now())
                else:
                    items.append(line[i])
            return items

        csvQueryBase = "COPY ({}) TO STDOUT WITH (FORMAT CSV)"

        for name in self.sourceData:
            fileLocation = os.path.join(self.tempDir, name + '.csv')
            columnNames, columnTypes = self.sourceData[name]

            if not os.path.exists(fileLocation):
                columns = sql.Composed(sql.SQL(column) for column in columnNames)
                tableQuery = sql.SQL("""SELECT {0} FROM {1}""").format(columns.join(', '), sql.SQL(name))
                csvQueryFull = sql.SQL(csvQueryBase).format(tableQuery)
                print(csvQueryFull.as_string(psqlObj.conn))

                with open(fileLocation, mode='ab') as f:
                    psqlObj.cur.copy_expert(csvQueryFull, f)

            with open(fileLocation, mode='r') as f:
                csvLines = csv.reader(f.readlines(), quotechar='"', delimiter=',',
                                      quoting=csv.QUOTE_ALL, skipinitialspace=True)
                csvTuples = list(map(tuple, csvLines))

            # TODO: Add input that limits number of csv entries used
            # Only work with 100 contributions, comment out to ignore
            if name == 'individual_contributions':
                csvTuples = csvTuples[:100]

            rdd = sqlCtx\
                .sparkSession\
                .sparkContext\
                .parallelize(csvTuples, numSlices=self.slices)\
                .map(convertItems)

            schema = StructType()
            for fieldName, fieldType in zip(*self.sourceData[name]):
                schema.add(StructField(fieldName, fieldType(), True))

            self.dataFrames[name] = sqlCtx.createDataFrame(rdd, schema)
            print('{0}: {1}'.format(name, self.dataFrames[name].count()))

            # Only work with 100 contributions, comment out to ignore
            if name == 'individual_contributions':
                self.dataFrames[name].show(n=100)

    def display(self, df, label):
        if self.debug:
            parentFunc = inspect.stack()[1][3]
            print('{0}: {1}'.format(parentFunc, label))
            df.show(5)

    def getAllValuesFromColumn(self, column, df=None):
        if not df:
            df = self.dataFrames['individual_contributions']
        self.display(df, 'initial DF')
        columnDF = df.select(column)
        self.display(columnDF, 'columnDF')
        uniqueColumnDF = columnDF.distinct()
        self.display(uniqueColumnDF, 'uniqueColumnDF')
        return uniqueColumnDF

    def getAllPartiesFromContributions(self, df):
        self.display(df, 'initial DF')
        recipientsDF = df.select('recipient_id')
        self.display(recipientsDF, 'recipientsDF')
        uniqueRecipientsDF = recipientsDF.distinct()
        self.display(uniqueRecipientsDF, 'uniqueRecipientsDF')
        uniqueCandidateRecipientsDF = uniqueRecipientsDF.filter(uniqueRecipientsDF['recipient_id'].startswith('N'))
        self.display(uniqueCandidateRecipientsDF, 'uniqueCandidateRecipientsDF')
        uniqueCommitteeRecipientsDF = uniqueRecipientsDF.filter(uniqueRecipientsDF['recipient_id'].startswith('C'))
        self.display(uniqueCommitteeRecipientsDF, 'uniqueCommitteeRecipientsDF')
        candidatesDF = self.dataFrames['candidates']
        self.display(candidatesDF, 'candidatesDF')
        committeesDF = self.dataFrames['committees']
        self.display(committeesDF, 'committeesDF')
        candidateRecipientsDF = candidatesDF.join(
            uniqueCandidateRecipientsDF, on=candidatesDF['cid'] == uniqueCandidateRecipientsDF['recipient_id'])
        self.display(candidateRecipientsDF, 'candidateRecipientsDF')
        committeeRecipientsDF = committeesDF.join(
            uniqueCommitteeRecipientsDF, on=committeesDF['committee_id'] == uniqueCommitteeRecipientsDF['recipient_id'])
        self.display(committeeRecipientsDF, 'committeeRecipientsDF')
        candidatePartiesDF = candidateRecipientsDF.select('party').distinct()
        self.display(candidatePartiesDF, 'candidatePartiesDF')
        committeePartiesDF = committeeRecipientsDF.select('party').distinct()
        self.display(committeePartiesDF, 'committeePartiesDF')
        return candidatePartiesDF, committeePartiesDF

    def splitContributionsByCycle(self):
        cyclesDF = self.getAllValuesFromColumn(column='cycle')

        dataFramesByCycle = {}

        for cycleRow in cyclesDF.collect():
            cycle = cycleRow['cycle']
            dataFramesByCycle[cycle] = self.dataFrames['individual_contributions']\
                .filter('cycle = {}'.format(cycle))
            self.display(dataFramesByCycle[cycle], 'cycle {0} DF'.format(cycle))

        return dataFramesByCycle

    def splitContributionsByPartyByCycle(self):
        dataFramesByPartyByCycle = {}
        if self.contributionsByCycle is None:
            self.contributionsByCycle = self.splitContributionsByCycle()
        cyclesDF = self.contributionsByCycle

        for cycle in cyclesDF:
            cycleDF = cyclesDF[cycle]
            self.display(cycleDF, 'cycle {} DF'.format(cycle))
            dataFramesByPartyByCycle[cycle] = {}
            candidatePartiesDF, committeePartiesDF = self.getAllPartiesFromContributions(cycleDF)
            for partyRow in candidatePartiesDF.union(committeePartiesDF).distinct().collect():
                party = partyRow['party']
                candidatesInPartyDF = self.dataFrames['candidates'].filter("party = '{}'".format(party))
                committeesInPartyDF = self.dataFrames['committees'].filter("party = '{}'".format(party))\
                    .withColumnRenamed("committee_id", "committeeId")
                contributionsForCandidatesInPartyDF = cycleDF \
                    .join(candidatesInPartyDF.select('cid'),
                          on=cycleDF['recipient_id'] == candidatesInPartyDF['cid']) \
                    .select(cycleDF.columns)
                self.display(contributionsForCandidatesInPartyDF, 'contributionsForCandidatesInPartyDF')
                contributionsForCommitteesInPartyDF = cycleDF \
                    .join(committeesInPartyDF,
                          on=cycleDF['recipient_id'] == committeesInPartyDF['committeeId']) \
                    .select(cycleDF.columns)
                self.display(contributionsForCommitteesInPartyDF, 'contributionsForCommitteesInPartyDF')
                dataFramesByPartyByCycle[cycle][party] = contributionsForCandidatesInPartyDF \
                    .union(contributionsForCommitteesInPartyDF) \
                    .distinct()
                self.display(dataFramesByPartyByCycle[cycle][party], "cycle '{0}' party '{1}' DF".format(cycle, party))

        return dataFramesByPartyByCycle

    def splitContributionsByTopCandidatesByCycle(self):
        pass

    def splitContributionsByTopCommitteesByCycle(self):
        pass

    def splitContributionsByIndustryByCycle(self):
        pass

    @staticmethod
    def totalContributed(df):
        return df.agg({"amount": "sum"}).collect()[0]['sum(amount)'] or 0

    @staticmethod
    def totalContributors(df):
        return df.select('contributor_id').distinct().count() or 0

    @staticmethod
    def averageContribution(df):
        average = df.agg({"amount": "avg"}).collect()[0]['avg(amount)']
        if average is not None:
            return round(average)
        else:
            return 0

    def getGenderContributors(self, gender, df=None):
        if not df:
            df = self.dataFrames['individual_contributions']
        return df.filter("gender = '{}'".format(gender))

    def getFirstCycleContributions(self, df=None):

        appliedSchema = StructType([
            StructField('contrib_id', sType(), False),
            StructField('firstCycle', iType(), False),
        ])

        @pandas_udf(appliedSchema, functionType=PandasUDFType.GROUPED_MAP)
        def getEarliestDate(pdf):
            contrib_id = pdf['contributor_id'].iloc[0]
            firstCycle = pdf.cycle.min()
            return pd.DataFrame([[contrib_id, firstCycle]])

        if not df:
            df = self.dataFrames['individual_contributions']

        contributionsByContributors = df.groupBy('contributor_id')

        firstCyclesDF = contributionsByContributors.apply(getEarliestDate)
        self.display(firstCyclesDF, 'firstCyclesDF')
        firstCycleContributions = df.join(
            firstCyclesDF, on=[df['contributor_id'] == firstCyclesDF['contrib_id'],
                               df['cycle'] == firstCyclesDF['firstCycle']]).select(df.columns)

        return firstCycleContributions

    def getContributionsByCycleBySize(self, largeSize, df=None):

        appliedSchema = StructType([
            StructField('contributorId', sType(), False),
            StructField('currentCycle', iType(), False),
            StructField('totalAmount', iType(), False)
        ])

        @pandas_udf(appliedSchema, functionType=PandasUDFType.GROUPED_MAP)
        def totalContributions(pdf):
            contrib_id = pdf['contributor_id'].iloc[0]
            currentCycle = pdf['cycle'].iloc[0]
            totalAmount = pdf.amount.sum()
            return pd.DataFrame([[contrib_id, currentCycle, totalAmount]])

        if largeSize:
            sizeRequirement = "totalAmount > 2000"
        else:
            sizeRequirement = "totalAmount < 100"

        if not df:
            df = self.dataFrames['individual_contributions']
        self.display(df, "Initial DF")

        contributionsByContributorsByCycle = df.groupBy(['contributor_id', 'cycle'])

        totalContributionByContributorsByCycle = contributionsByContributorsByCycle.apply(totalContributions)
        self.display(totalContributionByContributorsByCycle, "Total Contributions")

        contributorsBySize = totalContributionByContributorsByCycle.filter(sizeRequirement)
        self.display(contributorsBySize, 'contributorsBySize')

        contributionsBySize = df.join(contributorsBySize,
                                      on=[df['contributor_id'] == contributorsBySize['contributorId'],
                                          df['cycle'] == contributorsBySize['currentCycle']]).select(df.columns)
        self.display(contributionsBySize, 'contributionsBySize')

        return contributionsBySize

    def generateByCycleReport(self, sqlContext, schema):
        cycleSchema = StructType([StructField("cycle", iType(), False)])
        for struct in schema:
            cycleSchema.add(struct)
        byCycleStats = []
        if self.contributionsByCycle is None:
            self.contributionsByCycle = self.splitContributionsByCycle()
        for cycle in self.contributionsByCycle:
            self.display(self.contributionsByCycle[cycle], "Contributions for cycle {}".format(cycle))
            print("Creating stats for the {} cycle".format(cycle))
            byCycleStats.append({**{'cycle': cycle},
                                **self.generateStats(self.contributionsByCycle[cycle])})
        return sqlContext.createDataFrame(byCycleStats, cycleSchema)

    def generateByCycleByPartyReport(self, sqlContext, schema):
        cyclePartySchema = StructType([
            StructField("cycle", iType(), False),
            StructField("party", sType(), False)
        ])
        for struct in schema:
            cyclePartySchema.add(struct)
        byCycleByPartyStats = []
        contributionsByCycleByParty = self.splitContributionsByPartyByCycle()
        for cycle in contributionsByCycleByParty:
            for party in contributionsByCycleByParty[cycle]:
                byCycleByPartyStats.append({**{'cycle': cycle, 'party': party},
                                           **self.generateStats(contributionsByCycleByParty[cycle][party])})
        return sqlContext.createDataFrame(byCycleByPartyStats, cyclePartySchema)

    def generateTopCandidatesByCycleReport(self):
        pass

    def generateTopCommitteesByCycleReport(self):
        pass

    def generateByIndustryByCycleReport(self):
        pass

    def generateStats(self, df):
        stats = {}
        self.dataFrames['demographics'] = dict()
        for column in self.reportColumns:
            columnName, aggregator, demographic, parameter = column
            if demographic:
                demographicId = "{0}_{1}".format(demographic.__name__, parameter)
            else:
                demographicId = "all"
            print("Generating stats for the {} column".format(columnName))
            if not self.dataFrames['demographics'].get(demographicId, False):
                if demographic:
                    if parameter is not None:
                        appliedDF = demographic(parameter, df=df)
                    else:
                        appliedDF = demographic(df=df)
                else:
                    appliedDF = df
                self.dataFrames['demographics'][demographicId] = appliedDF
            stats[columnName] = aggregator(self.dataFrames['demographics'][demographicId])
        return stats

    def generateReports(self, sqlContext):
        commonSchema = StructType()
        for column in self.reportColumns:
            try:
                commonSchema.add(column[0], iType(), False)
            except AssertionError as ae:
                print(ae)

        # TODO: Implement remaining generators
        for reportGenerator in [self.generateByCycleReport,
                                self.generateByCycleByPartyReport,
                                # self.generateTopCandidatesByCycleReport,
                                # self.generateTopCommitteesByCycleReport,
                                # self.generateByIndustryByCycleReport
                                ]:
            reportName = reportGenerator.__name__[8:-6]
            print("Generating report {}".format(reportName))
            report = reportGenerator(sqlContext, commonSchema)
            self.display(report, reportName)
            yield (reportName, report)


class ETLChallenge:

    def __init__(self, args):
        PsqlObject.checkForJar()
        defaultArgs = ['800', False]
        for i in range(len(args), len(defaultArgs)):
            args.append(defaultArgs[i])
        slices, debug = args

        sc = SparkContext(master='local[*]', appName="ETLChallenge")
        config = sc.getConf()
        for key, value in config.getAll():
            print('{0}: {1}'.format(key, value))
        sqlContext = SQLContext(sc)
        sqlContext.clearCache()

        psqlObj = PsqlObject()
        psqlObj.printHeaders()
        dataObj = DataObject(slices=int(slices), debug=debug)

        with dataObj:
            dataObj.createDataFramesFromPsql(sqlContext, psqlObj)
            print(dataObj.dataFrames)

        for reportName, report in dataObj.generateReports(sqlContext):
            psqlObj.writeTable(reportName, report)


if __name__ == '__main__':
    ETLChallenge(sys.argv[1:])
