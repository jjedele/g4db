/*
 * This map/reduce script aggregates the sales by country.
 */

function map(emit, key, value) {
    var order = JSON.parse(value);
    var positions = order.positions;
    var orderTotal = 0;
    for (var i in positions) {
        orderTotal += positions[i].quantity * positions[i].unit_price;
    }
    emit(order.country, orderTotal.toString());
}

function reduce(values) {
    var total = 0;
    for (var i in values) {
        total += parseFloat(values[i]);
    }
    return total.toString();
}