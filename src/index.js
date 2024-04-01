const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');


// Helper functions for different JOIN types
function performInnerJoin(data, joinData, joinCondition, fields, table) {
    // Logic for INNER JOIN
    // ...
    return data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}


function performLeftJoin(data, joinData, joinCondition, fields, table) {
    return data.flatMap(mainRow => {
        const matchingJoinRows = joinData.filter(joinRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });

        if(matchingJoinRows.length > 0) {
            return matchingJoinRows.map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
        } else {
            // If no matching rows found in joinData, include mainRow with null values
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                if (tableName === table) {
                    acc[field] = mainRow[fieldName];
                } else {
                    acc[field] = null;
                }
                return acc;
            }, {});
        }
    });
}



function performRightJoin(data, joinData, joinCondition, fields, table) {
    return joinData.flatMap(joinRow => {
        const matchingDataRows = data.filter(mainRow => {
            const mainValue = mainRow[joinCondition.left.split('.')[1]];
            const joinValue = joinRow[joinCondition.right.split('.')[1]];
            return mainValue === joinValue;
        });

        if (matchingDataRows.length > 0) {
            // If there are matching rows, combine them
            return matchingDataRows.map(mainRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
        } else {
            // If no matching rows found in data, include joinRow with null values
            return fields.reduce((acc, field) => {
                const [tableName, fieldName] = field.split('.');
                if (tableName === table) {
                    acc[field] = null;
                } else {
                    acc[field] = joinRow[fieldName];
                }
                return acc;
            }, {});
        }
    });
}

// Helper function to apply GROUP BY and aggregate functions
function applyGroupBy(data, groupByFields, aggregateFunctions) {
    const groupResults = {};

    data.forEach(row => {
        // Generate a key for the group
        const groupKey = groupByFields.map(field => row[field]).join('-');

        // Initialize group in results if it doesn't exist
        if (!groupResults[groupKey]) {
            groupResults[groupKey] = { count: 0, sums: {}, mins: {}, maxes: {} };
            groupByFields.forEach(field => groupResults[groupKey][field] = row[field]);
        }

        // Aggregate calculations
        groupResults[groupKey].count += 1;
        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\w+)\)/.exec(func);
            if (match) {
                const [, aggFunc, aggField] = match;
                const value = parseFloat(row[aggField]);

                switch (aggFunc.toUpperCase()) {
                    case 'SUM':
                        groupResults[groupKey].sums[aggField] = (groupResults[groupKey].sums[aggField] || 0) + value;
                        break;
                    case 'MIN':
                        groupResults[groupKey].mins[aggField] = Math.min(groupResults[groupKey].mins[aggField] || value, value);
                        break;
                    case 'MAX':
                        groupResults[groupKey].maxes[aggField] = Math.max(groupResults[groupKey].maxes[aggField] || value, value);
                        break;
                    // Additional aggregate functions can be added here
                }
            }
        });
    });

    // Convert grouped results into an array format
    const resultArray = Object.values(groupResults).map(group => {
        // Construct the final grouped object based on required fields
        const finalGroup = {};
        groupByFields.forEach(field => finalGroup[field] = group[field]);
        aggregateFunctions.forEach(func => {
            const match = /(\w+)\((\*|\w+)\)/.exec(func);
            if (match) {
                const [, aggFunc, aggField] = match;
                switch (aggFunc.toUpperCase()) {
                    case 'SUM':
                        finalGroup[func] = group.sums[aggField];
                        break;
                    case 'MIN':
                        finalGroup[func] = group.mins[aggField];
                        break;
                    case 'MAX':
                        finalGroup[func] = group.maxes[aggField];
                        break;
                    case 'COUNT':
                        finalGroup[func] = group.count;
                        break;
                    // Additional aggregate functions can be handled here
                }
            }
        });

        return finalGroup;
    });
    return resultArray;
}


async function executeSELECTQuery(query) {
const { fields, table, whereClauses,joinType, joinTable, joinCondition,groupByFields,hasAggregateWithoutGroupBy } = parseQuery(query);
let data = await readCSV(`${table}.csv`);



    // Logic for applying JOINs
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            // Handle default case or unsupported JOIN types
        }
    }

    if (groupByFields) {
        data = applyGroupBy(data, groupByFields, fields);
    }


// Apply WHERE clause filtering after JOIN (or on the original data if no join)
const filteredData = whereClauses.length > 0
    ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
    : data;

    let groupResults = filteredData;
    console.log({ hasAggregateWithoutGroupBy });
    if (hasAggregateWithoutGroupBy) {
        // Special handling for queries like 'SELECT COUNT(*) FROM table'
        const result = {};

        console.log({ filteredData })

        fields.forEach(field => {
            const match = /(\w+)\((\*|\w+)\)/.exec(field);
            if (match) {
                const [, aggFunc, aggField] = match;
                switch (aggFunc.toUpperCase()) {
                    case 'COUNT':
                        result[field] = filteredData.length;
                        break;
                    case 'SUM':
                        result[field] = filteredData.reduce((acc, row) => acc + parseFloat(row[aggField]), 0);
                        break;
                    case 'AVG':
                        result[field] = filteredData.reduce((acc, row) => acc + parseFloat(row[aggField]), 0) / filteredData.length;
                        break;
                    case 'MIN':
                        result[field] = Math.min(...filteredData.map(row => parseFloat(row[aggField])));
                        break;
                    case 'MAX':
                        result[field] = Math.max(...filteredData.map(row => parseFloat(row[aggField])));
                        break;
                    // Additional aggregate functions can be handled here
                }
            }
        });
        return [result];
        // Add more cases here if needed for other aggregates
    } else if (groupByFields) {
        groupResults = applyGroupBy(filteredData, groupByFields, fields);
        return groupResults;
    } else {
        // Select the specified fields
        return groupResults.map(row => {
            const selectedRow = {};
            fields.forEach(field => {
                // Assuming 'field' is just the column name without table prefix
                selectedRow[field] = row[field];
            });
            return selectedRow;
        });
    }


    /*const selectedData=filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            // Assuming 'field' is just the column name without table prefix
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
    //Return the selected data
    return selectedData;
    */
   
    
}




function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = executeSELECTQuery;