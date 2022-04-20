
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
	
// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
// 
// note that this implementation only works for two-table queries that do not have an aggregation
// 
LogicalOpPtr SFWQuery :: buildLogicalQueryPlan (map <string, MyDB_TablePtr> &allTables, map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
                                                MyDB_BufferManagerPtr myMgr) {

	if (tablesToProcess.size () > 2) {
		cout << "Sorry, this currently only works for less-or-equals-two-table queries!\n";
		return nullptr;
	}

    bool areAggs = false;
    for (auto a: valuesToSelect) {
        if (a->hasAgg()) {
            areAggs = true;
        }
    }

    if (tablesToProcess.size() == 1) {
        // there is aggregation in the sql
        if (groupingClauses.size() != 0 || areAggs) {
            return buildLogicalOneTableWithAggQueryPlan(allTables, allTableReaderWriters, myMgr);
        }

        else {
            return buildLogicalOneTableQueryPlan(allTables, allTableReaderWriters, myMgr);
        }
    }

    else if (tablesToProcess.size() == 2) {
        return buildLogicalMultipleTablesPlan(allTables, allTableReaderWriters, myMgr);
    }

}

LogicalOpPtr SFWQuery::buildLogicalOneTableQueryPlan(map<string, MyDB_TablePtr> &allTables,
                                                     map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
                                                     MyDB_BufferManagerPtr myMgr) {
    MyDB_TablePtr inputTable = allTables[tablesToProcess[0].first];

    MyDB_SchemaPtr outputSchema = make_shared<MyDB_Schema>();
    vector<string> outputExpr;

    for (auto b : inputTable->getSchema()->getAtts()) {
        bool needIt = false;
        for (auto a : valuesToSelect) {
            if (a->referencesAtt(tablesToProcess[0].second, b.first)){
                needIt = true;
            }
        }

        if (needIt) {
            outputSchema->getAtts().push_back(b);
            outputExpr.push_back("[" + b.first + "]");
            cout << "output expr: " << ("[" + b.first + "]") << "\n";
        }
    }

    LogicalOpPtr tableScan = make_shared<LogicalTableScan>(allTableReaderWriters[tablesToProcess[0].first],
                                                               make_shared<MyDB_Table>("outputTable", "outputStorageLoc", outputSchema),
                                                               make_shared<MyDB_Stats>(inputTable, tablesToProcess[0].second),
                                                               allDisjunctions, outputExpr, myMgr);

    return tableScan;
}

LogicalOpPtr SFWQuery ::buildLogicalOneTableWithAggQueryPlan(map<string, MyDB_TablePtr> &allTables,
                                                             map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
                                                             MyDB_BufferManagerPtr myMgr) {

    // first, we need to do regular selection
    MyDB_TablePtr inputTable = allTables[tablesToProcess[0].first];

    MyDB_SchemaPtr selectionSchema = make_shared<MyDB_Schema>();
    vector<string> selectionExpr;

    for (auto b : inputTable->getSchema()->getAtts()) {
        bool needIt = false;
        for (auto a : valuesToSelect) {
            if (a->referencesAtt(tablesToProcess[0].second, b.first)){
                needIt = true;
            }
        }

        if (needIt) {
            selectionSchema->getAtts().push_back(b);
            selectionExpr.push_back("[" + b.first + "]");
            cout << "output expr: " << ("[" + b.first + "]") << "\n";
        }
    }

    LogicalOpPtr selectionScan = make_shared<LogicalTableScan>(allTableReaderWriters[tablesToProcess[0].first],
                                                           make_shared<MyDB_Table>("selectionTable", "selectionStorageLoc", selectionSchema),
                                                           make_shared<MyDB_Stats>(inputTable, tablesToProcess[0].second),
                                                           allDisjunctions, selectionExpr, myMgr);

    // do aggregation from selectionTable
    MyDB_SchemaPtr outputSchema = make_shared<MyDB_Schema>();

    MyDB_Record myRec(selectionSchema);

    int i = 0;
    for (auto a : valuesToSelect) {
        if (a->isSum()) {
            outputSchema->getAtts().push_back(make_pair("att_" + to_string(i++), myRec.getType(a->getChild()->toString())));
        }

        else if (a->isAvg()) {
            outputSchema->getAtts().push_back(make_pair("att_" + to_string(i++), make_shared <MyDB_DoubleAttType> ()));
        }

        else {
            outputSchema->getAtts().push_back(make_pair("att_" + to_string(i++), myRec.getType(a->toString())));
        }
    }

    LogicalOpPtr aggregate = make_shared<LogicalAggregate>(selectionScan, make_shared<MyDB_Table>("outputTable", "outputStorageLoc", outputSchema),
                                                           valuesToSelect, groupingClauses, myMgr);

    return aggregate;
}

LogicalOpPtr SFWQuery::buildLogicalMultipleTablesPlan(map<string, MyDB_TablePtr> &allTables,
                                                 map<string, MyDB_TableReaderWriterPtr> &allTableReaderWriters,
                                                 MyDB_BufferManagerPtr myMgr) {

    // find the two input tables
    MyDB_TablePtr leftTable = allTables[tablesToProcess[0].first];
    MyDB_TablePtr rightTable = allTables[tablesToProcess[1].first];

    // find the various parts of the CNF
    vector<ExprTreePtr> leftCNF;
    vector<ExprTreePtr> rightCNF;
    vector<ExprTreePtr> topCNF;

    // loop through all of the disjunctions and break them apart
    for (auto a: allDisjunctions) {
        bool inLeft = a->referencesTable(tablesToProcess[0].second);
        bool inRight = a->referencesTable(tablesToProcess[1].second);
        if (inLeft && inRight) {
            cout << "top " << a->toString() << "\n";
            topCNF.push_back(a);
        } else if (inLeft) {
            cout << "left: " << a->toString() << "\n";
            leftCNF.push_back(a);
        } else {
            cout << "right: " << a->toString() << "\n";
            rightCNF.push_back(a);
        }
    }

    // now get the left and right schemas for the two selections
    MyDB_SchemaPtr leftSchema = make_shared<MyDB_Schema>();
    MyDB_SchemaPtr rightSchema = make_shared<MyDB_Schema>();
    MyDB_SchemaPtr totSchema = make_shared<MyDB_Schema>();
    vector<string> leftExprs;
    vector<string> rightExprs;
    vector<string> totExprs;

    // and see what we need from the left, and from the right
    for (auto b: leftTable->getSchema()->getAtts()) {
        bool needIt = false;
        for (auto a: valuesToSelect) {
            if (a->referencesAtt(tablesToProcess[0].second, b.first)) {
                needIt = true;
            }
        }
        for (auto a: topCNF) {
            if (a->referencesAtt(tablesToProcess[0].second, b.first)) {
                needIt = true;
            }
        }
        if (needIt) {
            leftSchema->getAtts().push_back(make_pair(b.first, b.second));
            totSchema->getAtts().push_back(make_pair(b.first, b.second));
            leftExprs.push_back("[" + b.first + "]");
            totExprs.push_back("[" + b.first + "]");
            cout << "left expr: " << ("[" + b.first + "]") << "\n";
        }
    }

    cout << "left schema: " << leftSchema << "\n";

    // and see what we need from the right, and from the right
    for (auto b: rightTable->getSchema()->getAtts()) {
        bool needIt = false;
        for (auto a: valuesToSelect) {
            if (a->referencesAtt(tablesToProcess[1].second, b.first)) {
                needIt = true;
            }
        }
        for (auto a: topCNF) {
            if (a->referencesAtt(tablesToProcess[1].second, b.first)) {
                needIt = true;
            }
        }
        if (needIt) {
            rightSchema->getAtts().push_back(make_pair(b.first, b.second));
            totSchema->getAtts().push_back(make_pair(b.first, b.second));
            rightExprs.push_back("[" + b.first + "]");
            totExprs.push_back("[" + b.first + "]");
            cout << "right expr: " << ("[" + b.first + "]") << "\n";
        }
    }
    cout << "right schema: " << rightSchema << "\n";

    // now we gotta figure out the top schema... get a record for the top
    MyDB_Record myRec(totSchema);

    // and it's time to build the query plan
    LogicalOpPtr leftTableScan = make_shared<LogicalTableScan>(allTableReaderWriters[tablesToProcess[0].first],
                                                               make_shared<MyDB_Table>("leftTable", "leftStorageLoc", leftSchema),
                                                               make_shared<MyDB_Stats>(leftTable, tablesToProcess[0].second),
                                                               leftCNF, leftExprs, myMgr);
    LogicalOpPtr rightTableScan = make_shared<LogicalTableScan>(allTableReaderWriters[tablesToProcess[1].first],
                                                                make_shared<MyDB_Table>("rightTable", "rightStorageLoc", rightSchema),
                                                                make_shared<MyDB_Stats>(rightTable, tablesToProcess[1].second),
                                                                rightCNF, rightExprs, myMgr);

    bool areAggs = false;
    for (auto a: valuesToSelect) {
        if (a->hasAgg()) {
            areAggs = true;
        }
    }

    if (groupingClauses.size() != 0 || areAggs) {
        // there is aggregations in sql

        MyDB_SchemaPtr outputSchema = make_shared<MyDB_Schema>();
        int i = 0;
        for (auto a : valuesToSelect) {
            if (a->isSum()) {
                outputSchema->getAtts().push_back(make_pair("att_" + to_string(i++), myRec.getType(a->getChild()->toString())));
            }

            else if (a->isAvg()) {
                outputSchema->getAtts().push_back(make_pair("att_" + to_string(i++), make_shared <MyDB_DoubleAttType> ()));
            }

            else {
                outputSchema->getAtts().push_back(make_pair("att_" + to_string(i++), myRec.getType(a->toString())));
            }
        }

        LogicalOpPtr joinResult = make_shared<LogicalJoin>(leftTableScan, rightTableScan,
                                                            make_shared<MyDB_Table>("joinTable", "joinStorageLoc", totSchema),
                                                                    topCNF, totExprs, myMgr);

        LogicalOpPtr aggregatePlan = make_shared<LogicalAggregate>(joinResult, make_shared<MyDB_Table>("outputTable", "outputStorageLoc", outputSchema),
                                                                   valuesToSelect, groupingClauses, myMgr);

        return aggregatePlan;
    }
    else {
        // there is no aggregation in sql

        // and get all of the attributes for the output
        MyDB_SchemaPtr topSchema = make_shared<MyDB_Schema>();
        int i = 0;
        for (auto a: valuesToSelect) {
            topSchema->getAtts().push_back(make_pair("att_" + to_string(i++), myRec.getType(a->toString())));
        }
        cout << "top schema: " << topSchema << "\n";

        vector <string> exprsToCompute;
        for (auto a : valuesToSelect) {
            exprsToCompute.push_back(a->toString());
        }

        LogicalOpPtr returnVal = make_shared<LogicalJoin>(leftTableScan, rightTableScan,
                                                          make_shared<MyDB_Table>("topTable", "topStorageLoc", topSchema),
                                                          topCNF, exprsToCompute, myMgr);

        // done!!
        return returnVal;
    }

}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
	allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
