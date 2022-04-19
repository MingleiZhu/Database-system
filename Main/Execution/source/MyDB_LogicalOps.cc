
#ifndef LOG_OP_CC
#define LOG_OP_CC

#include "MyDB_LogicalOps.h"

using namespace std;

// fill this out!  This should actually run the aggregation via an appropriate RelOp, and then it is going to
// have to unscramble the output attributes and compute exprsToCompute using an execution of the RegularSelection 
// operation (why?  Note that the aggregate always outputs all of the grouping atts followed by the agg atts.
// After, a selection is required to compute the final set of aggregate expressions)
//
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalAggregate :: execute () {
    MyDB_TableReaderWriterPtr selectionTable = inputOp->execute();

    vector <pair <MyDB_AggType, string>> aggsToCompute;
    vector <string> groups;

    for (auto a : exprsToCompute) {
        if (a->isAvg()) {
            aggsToCompute.push_back(make_pair(MyDB_AggType::Avg, "* (" + a->getChild()->toString() + ", double[1.0])"));
        }
        else if (a->isSum()) {
            aggsToCompute.push_back(make_pair(MyDB_AggType::Sum, a->getChild()->toString()));
        }
    }

    for (auto b : groupings) {
        groups.push_back(b->toString());
    }

    MyDB_SchemaPtr aggregateSchema = make_shared<MyDB_Schema>();

    MyDB_RecordPtr myRec = selectionTable->getEmptyRecord();
    for (string g : groups) {
        aggregateSchema->appendAtt(make_pair(g.substr(1, g.length() - 2), myRec->getType(g)));
    }

    for (auto a : exprsToCompute) {
        if (a->isAvg()) {
            aggregateSchema->appendAtt(make_pair(to_string(hash<string>()(a->toString())), make_shared <MyDB_DoubleAttType> ()));
        }
        else if (a->isSum()) {
            aggregateSchema->appendAtt(make_pair(to_string(hash<string>()(a->toString())), myRec->getType(a->getChild()->toString())));
        }
    }



    MyDB_TableReaderWriterPtr aggregationTable = make_shared<MyDB_TableReaderWriter>(
            make_shared<MyDB_Table>("aggregateTable", "aggregateLoc", aggregateSchema), myMgr);

    Aggregate aggOp(selectionTable, aggregationTable, aggsToCompute, groups, "bool[true]");
    aggOp.run();

    // regular selection from aggregate table
    MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, myMgr);

    vector<string> projections;
    for (auto a : exprsToCompute) {
        if (a->isSum() || a->isAvg()) {
            projections.push_back("[" + to_string(hash<string>()(a->toString()))+ "]");
        }
        else {
            projections.push_back(a->toString());
        }
    }

    RegularSelection rsOp(aggregationTable, outputTable, "bool[true]", projections);
    rsOp.run();

    myMgr->killTable(selectionTable->getTable());
    myMgr->killTable(aggregationTable->getTable());

    return outputTable;
}
// we don't really count the cost of the aggregate, so cost its subplan and return that
pair <double, MyDB_StatsPtr> LogicalAggregate :: cost () {
	return inputOp->cost ();
}
	
// this costs the entire query plan with the join at the top, returning the compute set of statistics for
// the output.  Note that it recursively costs the left and then the right, before using the statistics from
// the left and the right to cost the join itself
pair <double, MyDB_StatsPtr> LogicalJoin :: cost () {
	auto left = leftInputOp->cost ();
	auto right = rightInputOp->cost ();
	MyDB_StatsPtr outputStats = left.second->costJoin (outputSelectionPredicate, right.second);
	return make_pair (left.first + right.first + outputStats->getTupleCount (), outputStats);
}
	
// Fill this out!  This should recursively execute the left hand side, and then the right hand side, and then
// it should heuristically choose whether to do a scan join or a sort-merge join (if it chooses a scan join, it
// should use a heuristic to choose which input is to be hashed and which is to be scanned), and execute the join.
// Note that after the left and right hand sides have been executed, the temporary tables associated with the two 
// sides should be deleted (via a kill to killFile () on the buffer manager)
MyDB_TableReaderWriterPtr LogicalJoin :: execute () {
	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, myMgr);

    MyDB_TableReaderWriterPtr leftTable = leftInputOp->execute();
    MyDB_TableReaderWriterPtr rightTable = rightInputOp->execute();

    int minPageNum = min (leftTable->getNumPages(), rightTable->getNumPages());

    vector<pair<string, string>> equalityChecks;
    string finalPredicate;
    vector<string> projections;
    for (auto& predicate : outputSelectionPredicate) {
        if (predicate->isEq()) {
            string lhs = predicate->getLHS()->toString();
            string rhs = predicate->getRHS()->toString();
            // if LHS of equality is attribute of left table
            if (leftTable->getTable()->getSchema()->getAttByName(lhs).first != -1) {
                equalityChecks.push_back(make_pair(lhs, rhs));
            }
            else {
                equalityChecks.push_back(make_pair(rhs, lhs));
            }
        }
    }

    if (outputSelectionPredicate.size() == 0) {
        finalPredicate = "bool[true]";
    }
    else {
        finalPredicate = outputSelectionPredicate.back()->toString();
        outputSelectionPredicate.pop_back();
        while (outputSelectionPredicate.size() >= 1) {
            finalPredicate = "&& (" + finalPredicate + ", " + outputSelectionPredicate.back()->toString() + ")";
            outputSelectionPredicate.pop_back();
        }
    }

    for (auto& value : exprsToCompute) {
        projections.push_back(value->toString());
    }

    if (minPageNum <= myMgr->getNumPages() / 2) {
        ScanJoin scanJoin(leftTable, rightTable, outputTable, finalPredicate, projections, equalityChecks, "bool[true]", "bool[true]");
        scanJoin.run();
    }
    else {
        SortMergeJoin sortMergeJoin(leftTable, rightTable, outputTable, finalPredicate, projections, equalityChecks.back(), "bool[true]", "bool[true]");
        sortMergeJoin.run();
    }

    myMgr->killTable(leftTable->getTable());
    myMgr->killTable(rightTable->getTable());
    return outputTable;

}

// this costs the table scan returning the compute set of statistics for the output
pair <double, MyDB_StatsPtr> LogicalTableScan :: cost () {
	MyDB_StatsPtr returnVal = inputStats->costSelection (selectionPred);
	return make_pair (returnVal->getTupleCount (), returnVal);	
}

// fill this out!  This should heuristically choose whether to use a B+-Tree (if appropriate) or just a regular
// table scan, and then execute the table scan using a relational selection.  Note that a desirable optimization
// is to somehow set things up so that if a B+-Tree is NOT used, that the table scan does not actually do anything,
// and the selection predicate is handled at the level of the parent (by filtering, for example, the data that is
// input into a join)
MyDB_TableReaderWriterPtr LogicalTableScan :: execute () {
	MyDB_TableReaderWriterPtr outputTable = make_shared<MyDB_TableReaderWriter>(outputSpec, myMgr);

    string predicate;

    // there is no prediction
    if (selectionPred.size() == 0) {
        predicate = "bool[true]";
    }
    else {
        predicate = selectionPred.back()->toString();
        selectionPred.pop_back();
        while (selectionPred.size() >= 1) {
            predicate = "&& (" + predicate + ", " + selectionPred.back()->toString() + ")";
            selectionPred.pop_back();
        }
    }

    RegularSelection myOp(inputSpec, outputTable, predicate, exprsToCompute);
    myOp.run();

    return outputTable;
}

#endif
