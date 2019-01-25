/*
 * This map/reduce script aggregates the sales by country.
 */

// First Usecase - Total Volume

var N = 10;

function map(emit, key, value) {
    var order = JSON.parse(value);
    var positions = order.positions;
    var orderTotal = 0;
    for (var i in positions) {
        orderTotal += positions[i].quantity * positions[i].unit_price;
    }

    var result = [{orderId: order.invoice_no, total: orderTotal}];
    emit("result", JSON.stringify(result));
}

function reduce(values) {
    var lists = [];
    for (var i in values) {
        lists.push(JSON.parse(values[i]));
    }

    var result = [];
    for (var el = 0; el < N; el++) {
        // for each of the top N positions
        // find the list with the maximal first element
        var currentMax = -1e10;
        var currentIdx = -1e10;
        for (var li in lists) {
            var list = lists[li];

            if (list.length == 0) {
                // since we always remove the largest element from the list. lists could be empty
                continue;
            }

            if (list[0].total > currentMax) {
                currentMax = list[0].total;
                currentIdx = li;
            }
        }

        if (currentIdx < 0) {
            // we have less than N elements in total, so we stop here and take what we have
            break;
        }

        // remove the maximal first element from its list and add it to the result
        result.push(lists[currentIdx].shift());
    }

    return JSON.stringify(result);
}