import os
import sys
import inspect
import io
from psycopg2 import sql
import pandas as pd
import numpy as np
import numpy.lib.recfunctions as rfn
from sqlalchemy import create_engine


class PsqlObject:
    def __init__(self):
        self.userName = 'postgres'
        self.password = 'postgres'
        self.hostName = 'localhost'
        self.database = 'campaign_finance'
        self.url = 'jdbc:postgresql://localhost/{}'.format(self.database)
        self.engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}/{3}'
                                    .format(self.userName, self.password, self.hostName, self.database))
        self.conn = self.engine.raw_connection().connection
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
        uniqueKeyNames = tableDF.columns.values[:-18].tolist()
        uniqueKeyTypes = tableDF.dtypes.values[:-18].tolist()
        columnPairs = []

        for i in range(len(uniqueKeyTypes)):
            keyType = uniqueKeyTypes[i]
            if keyType == np.int64:
                columnPairs.append((uniqueKeyNames[i], 'integer'))
            elif keyType == np.object_:
                columnPairs.append((uniqueKeyNames[i], 'varchar'))
            else:
                assert False, "Unrecognizable type \"{}\" found".format(keyType)

        for name in tableDF.columns.values[-18:].tolist():
            columnPairs.append((name, 'integer'))

        tableTypes = sql.Composed([sql.SQL('{0}\t{1}').format(sql.SQL(k), sql.SQL(t)) for k, t in columnPairs])\
            .join(', ')

        self.cur.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.SQL(tableName)))
        createQuery = sql.SQL("CREATE TABLE {0} ({1}, PRIMARY KEY ({2}));").format(
            sql.Identifier(tableName),
            tableTypes,
            sql.SQL(', ').join(list(map(sql.Identifier, uniqueKeyNames)))
        )
        print(createQuery.as_string(self.conn))
        self.cur.execute(createQuery)

        tableDF.head(0).to_sql(tableName, self.engine, if_exists='replace', index=False)

        output = io.StringIO()
        tableDF.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        output.getvalue()
        self.cur.copy_from(output, tableName, null="")  # null values become ''
        self.conn.commit()


class DataObject:
    def __init__(self, slices, debug):
        self.debug = debug
        self.tables = {}

        sType = 'O'
        iType = 'int'
        dType = 'datetime64[s]'

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
                [iType, sType,           sType,           sType]
            ],
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
        self.largeContributorsByCycle = None
        self.smallContributorsByCycle = None

    def __enter__(self):
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

    def createDataFramesFromPsql(self, psqlObj):
        for name in self.sourceData:
            columnNames, columnTypes = self.sourceData[name]

            columns = sql.Composed(sql.SQL(column) for column in columnNames)
            tableQuery = sql.SQL("""SELECT {0} FROM {1}""").format(columns.join(', '), sql.SQL(name))

            psqlObj.cur.execute(tableQuery)
            dataTypes = list(zip(*self.sourceData[name]))
            self.tables[name] = np.array(psqlObj.cur.fetchall(), dataTypes)
            fields = self.tables[name].dtype.fields

            for columnName in fields:
                if fields[columnName][0].char == 'O':
                    column = self.tables[name][columnName]
                    np.putmask(column, column == None, '')

    def display(self, table, label, entries=5):
        if self.debug:
            parentFunc = inspect.stack()[1][3]
            print('{0}: {1}'.format(parentFunc, label))
            print(table[:entries])

    def getAllValuesFromColumn(self, column, table=None):
        if table is None:
            table = self.tables['individual_contributions']
        self.display(table, 'initial table')
        columnTable = table[column]
        self.display(columnTable, 'columnTable')
        uniqueColumnTable = np.unique(columnTable)
        self.display(uniqueColumnTable, 'uniqueColumnTable')
        return uniqueColumnTable

    def getAllPartiesFromContributions(self, table):
        self.display(table, 'initial table')
        recipientsTable = table['recipient_id']
        self.display(recipientsTable, 'recipientsTable')
        uniqueRecipientsTable = np.unique(recipientsTable)
        self.display(uniqueRecipientsTable, 'uniqueRecipientsTable')
        uniqueCandidateRecipientsTable = np.array(
            list(filter(lambda recipient: recipient.startswith('N'), uniqueRecipientsTable)),
            dtype=[('recipient_id', 'O')]
        )
        self.display(uniqueCandidateRecipientsTable, 'uniqueCandidateRecipientsTable')
        uniqueCommitteeRecipientsTable = np.array(
            list(filter(lambda recipient: recipient.startswith('C'), uniqueRecipientsTable)),
            dtype=[('recipient_id', 'O')]
        )
        self.display(uniqueCommitteeRecipientsTable, 'uniqueCommitteeRecipientsTable')

        candidatesTable = self.tables['candidates'].copy()
        self.display(candidatesTable, 'candidatesTable')
        candidatesTableColumns = [column for column in self.sourceData['candidates'][0]]
        candidatesTableColumns[0] = 'recipient_id'
        candidatesTable.dtype.names = tuple(candidatesTableColumns)

        committeesTable = self.tables['committees'].copy()
        self.display(committeesTable, 'committeesTable')
        committeesTableColumns = [column for column in self.sourceData['committees'][0]]
        committeesTableColumns[0] = 'recipient_id'
        committeesTable.dtype.names = tuple(committeesTableColumns)

        candidateRecipientsTable = rfn.join_by('recipient_id', candidatesTable, uniqueCandidateRecipientsTable,
                                               jointype='inner', usemask=False)
        self.display(candidateRecipientsTable, 'candidateRecipientsTable')

        committeeRecipientsTable = rfn.join_by('recipient_id', committeesTable, uniqueCommitteeRecipientsTable,
                                               jointype='inner', usemask=False)
        self.display(committeeRecipientsTable, 'committeeRecipientsTable')

        candidatePartiesTable = np.unique(candidateRecipientsTable['party'])
        self.display(candidatePartiesTable, 'candidatePartiesTable')
        committeePartiesTable = np.unique(committeeRecipientsTable['party'])
        self.display(committeePartiesTable, 'committeePartiesTable')
        return candidatePartiesTable, committeePartiesTable

    def splitContributionsByCycle(self):
        cyclesTable = self.getAllValuesFromColumn(column='cycle')

        tablesByCycle = {}

        for cycle in cyclesTable:
            tablesByCycle[cycle] = np.array(
                list(filter(lambda contribution: contribution['cycle'] == cycle,
                            self.tables['individual_contributions'])),
                dtype=list(zip(*self.sourceData['individual_contributions']))
            )
            self.display(tablesByCycle[cycle], 'cycle {0} table'.format(cycle))

        return tablesByCycle

    def splitContributionsByPartyByCycle(self):
        tablesByPartyByCycle = {}
        if self.contributionsByCycle is None:
            self.contributionsByCycle = self.splitContributionsByCycle()

        for cycle in self.contributionsByCycle:
            cycleTable = self.contributionsByCycle[cycle]
            self.display(cycleTable, 'cycle {} table'.format(cycle))
            tablesByPartyByCycle[cycle] = {}
            candidatePartiesTable, committeePartiesTable = self.getAllPartiesFromContributions(cycleTable)
            partiesTable = np.concatenate((candidatePartiesTable, committeePartiesTable))
            for party in np.unique(partiesTable):
                candidatesInPartyTable = self.getAllValuesFromColumn(
                    'cid',
                    np.array(
                        list(filter(lambda candidate: candidate['party'] == party, self.tables['candidates'])),
                        dtype=list(zip(*self.sourceData['candidates']))
                    )
                )

                committeesInPartyTable = self.getAllValuesFromColumn(
                    'committee_id',
                    np.array(
                        list(filter(lambda committee: committee['party'] == party, self.tables['committees'])),
                        dtype=list(zip(*self.sourceData['committees']))
                    )
                )

                contributionsForCandidatesInPartyTable = np.array(
                    list(
                        filter(lambda contribution: contribution['recipient_id'] in candidatesInPartyTable, cycleTable)
                    ),
                    dtype=list(zip(*self.sourceData['individual_contributions']))
                )

                contributionsForCommitteesInPartyTable = np.array(
                    list(
                        filter(lambda contribution: contribution['recipient_id'] in committeesInPartyTable, cycleTable)
                    ),
                    dtype=list(zip(*self.sourceData['individual_contributions']))
                )

                self.display(contributionsForCandidatesInPartyTable, 'contributionsForCandidatesInPartyTable')

                self.display(contributionsForCommitteesInPartyTable, 'contributionsForCommitteesInPartyTable')

                tablesByPartyByCycle[cycle][party] = np.unique(
                    np.concatenate((contributionsForCandidatesInPartyTable, contributionsForCommitteesInPartyTable))
                )

                self.display(tablesByPartyByCycle[cycle][party], "cycle '{0}' party '{1}' table".format(cycle, party))

        return tablesByPartyByCycle

    def splitContributionsByTopCandidatesByCycle(self):
        pass

    def splitContributionsByTopCommitteesByCycle(self):
        pass

    def splitContributionsByIndustryByCycle(self):
        pass

    @staticmethod
    def totalContributed(table):
        return np.sum(table["amount"]).item()

    @staticmethod
    def totalContributors(table):
        return np.unique(table['contributor_id']).size

    @staticmethod
    def averageContribution(table):
        return np.sum(table["amount"]).item()

    def getGenderContributors(self, gender, table=None):
        if table is None:
            table = self.tables['individual_contributions']
        return np.array(
            list(
                filter(lambda contribution: contribution['gender'] == gender, table)
            ),
            dtype=list(zip(*self.sourceData['individual_contributions']))
        )
        # return df.filter("gender = '{}'".format(gender))

    def getFirstCycleContributions(self, table=None):
        if table is None:
            table = self.tables['individual_contributions']

        firstCycleContributions = np.empty((1,), dtype=list(zip(*self.sourceData['individual_contributions'])))

        contributors = self.getAllValuesFromColumn('contributor_id', table)

        for contributor in contributors:
            contributorTable = np.array(
                list(
                    filter(lambda contribution: contribution['contributor_id'] == contributor, table)
                ),
                dtype=list(zip(*self.sourceData['individual_contributions']))
            )
            contributorCycles = contributorTable['cycle']
            firstCycle = np.amin(contributorCycles)
            firstCycleTable = np.array(
                list(
                    filter(lambda contribution: contribution['cycle'] == firstCycle, contributorTable)
                ),
                dtype=list(zip(*self.sourceData['individual_contributions']))
            )
            self.display(firstCycleTable, '{} first contributions'.format(contributor))
            firstCycleContributions = np.vstack((firstCycleContributions, firstCycleTable[:, None]))

        if firstCycleContributions.size == 1:
            return firstCycleContributions[1:]
        return firstCycleContributions[1:, 0]

    def getContributionsByCycleBySize(self, largeSize, table=None):
        if table is None:
            table = self.tables['individual_contributions']
        self.display(table, "Initial table")

        cycleSizeContributions = np.empty((1,), dtype=list(zip(*self.sourceData['individual_contributions'])))

        contributors = self.getAllValuesFromColumn('contributor_id', table)

        for contributor in contributors:
            contributorTable = np.array(
                list(
                    filter(lambda contribution: contribution['contributor_id'] == contributor, table)
                ),
                dtype=list(zip(*self.sourceData['individual_contributions']))
            )
            contributorCycles = self.getAllValuesFromColumn('cycle', contributorTable)
            for cycle in contributorCycles:
                cycleContributorTable = np.array(
                    list(
                        filter(lambda contribution: contribution['cycle'] == cycle, contributorTable)
                    ),
                    dtype=list(zip(*self.sourceData['individual_contributions']))
                )
                cycleContributorSum = np.sum(cycleContributorTable['amount'])
                print('contributor {0} in cycle {1} contributed {2}'.format(contributor, cycle, cycleContributorSum))
                if (largeSize and cycleContributorSum > 2000) or (not largeSize and cycleContributorSum < 100):
                    cycleSizeContributions = np.vstack((cycleSizeContributions, cycleContributorTable[:, None]))
                    self.display(
                        cycleContributorTable,
                        'contributor {0} cycle {1} {2} contributions'.format(
                            contributor, cycle, ['small', 'large'][largeSize]
                        )
                    )

        if cycleSizeContributions.size == 1:
            return cycleSizeContributions[1:]
        return cycleSizeContributions[1:, 0]

    def generateByCycleReport(self):
        rows = []
        byCycleStats = []
        columnNames = ['cycle']
        columnNames.extend(list(zip(*self.reportColumns))[0])
        if self.contributionsByCycle is None:
            self.contributionsByCycle = self.splitContributionsByCycle()
        for cycle in self.contributionsByCycle:
            self.display(self.contributionsByCycle[cycle], "Contributions for cycle {}".format(cycle))
            rows.append(cycle)
            print("Creating stats for the {} cycle".format(cycle))
            byCycleStats.append({**{'cycle': cycle.item()},
                                **self.generateStats(self.contributionsByCycle[cycle])})
        return pd.DataFrame(byCycleStats, index=rows, columns=columnNames)

    def generateByCycleByPartyReport(self):
        rows = []
        byCycleByPartyStats = []
        contributionsByCycleByParty = self.splitContributionsByPartyByCycle()
        columnNames = ['cycle', 'party']
        columnNames.extend(list(zip(*self.reportColumns))[0])
        for cycle in contributionsByCycleByParty:
            for party in contributionsByCycleByParty[cycle]:
                print("Creating stats for the {0} party in the {1} cycle".format(party, cycle))
                rows.append((party, cycle.item()))
                byCycleByPartyStats.append({**{'cycle': cycle.item(), 'party': party},
                                           **self.generateStats(contributionsByCycleByParty[cycle][party])})
        return pd.DataFrame(byCycleByPartyStats, index=rows, columns=columnNames)

    def generateTopCandidatesByCycleReport(self):
        pass

    def generateTopCommitteesByCycleReport(self):
        pass

    def generateByIndustryByCycleReport(self):
        pass

    def generateStats(self, table):
        stats = {}
        self.tables['demographics'] = dict()
        for column in self.reportColumns:
            columnName, aggregator, demographic, parameter = column
            if demographic:
                demographicId = "{0}_{1}".format(demographic.__name__, parameter)
            else:
                demographicId = "all"
            print("Generating stats for the {} column".format(columnName))
            if self.tables['demographics'].get(demographicId, None) is None:
                if demographic:
                    if parameter is not None:
                        appliedDF = demographic(parameter, table=table)
                    else:
                        appliedDF = demographic(table=table)
                else:
                    appliedDF = table
                self.tables['demographics'][demographicId] = appliedDF
            stats[columnName] = aggregator(self.tables['demographics'][demographicId])
        return stats

    def generateReports(self):

        for reportGenerator in [self.generateByCycleReport,
                                self.generateByCycleByPartyReport,
                                # self.generateTopCandidatesByCycleReport,
                                # self.generateTopCommitteesByCycleReport,
                                # self.generateByIndustryByCycleReport
                                ]:
            reportName = reportGenerator.__name__[8:-6]
            print("Generating report {}".format(reportName))
            report = reportGenerator()
            print("{0}:\n{1}".format(reportName, report))
            yield (reportName, report)


class Fair:

    def __init__(self, args):
        defaultArgs = ['800', False]
        for i in range(len(args), len(defaultArgs)):
            args.append(defaultArgs[i])
        slices, debug = args

        psqlObj = PsqlObject()
        psqlObj.printHeaders()
        dataObj = DataObject(slices=int(slices), debug=debug)

        dataObj.createDataFramesFromPsql(psqlObj)

        for reportName, report in dataObj.generateReports():
            psqlObj.writeTable(reportName.lower(), report)


if __name__ == '__main__':
    Fair(sys.argv[1:])
