# Map/Reduce

## Data

With this repository comes a [example data set](demo_data/OnlineRetail.json.txt) of retail data and a
tool to ingest it into the database:

    $ java -cp client.jar app_kvClient.demo.OnlineRetailIngest $HOST:$PORT demo_data/OnlineRetail.json.txt $MAX_RECORDS

where `$MAX_RECORDS` is the maximum number of records to be ingested. Use _26000_ for the full data set.

Credits for the data set go to:
Chen, D., Sain, S. L., & Guo, K. (2012). Data mining for the online retail industry: A case study of RFM model-based customer segmentation using data mining. Journal of Database Marketing and Customer Strategy Management, 19(3), 197–208. https://doi.org/10.1057/dbm.2012.17

## Scripts

Also included es.jsare a couple of example map/reduce programs.

Map/reduce scripts are triggered via the usual CLI. There are two variants:

    G4DB> mapReduce target_ns local/path/to/script.js

Executes the given script on all records in the default namespace (i.e. no namespace) and writes
the results into the `target_ns` namespace.

    G4DB> mapReduce source_ns target_ns local/path/to/script.js

Executes the given script on all records in the `source_ns` namespace and can be used to develop
multi-staged map/reduce workflows.

### Custom Scripts

The map/reduce scripts are executed in the JDK's internal Nashorn JavaScript engine. Each script must
contain following two functions:

    function map(emit, key, value) {
      // processes one key-value-pair at a time
      // can emit arbirarily many derived pairs using
      //
      // emit(new_key, new_value);
    }

    function reduce(key, values) {
      // reduces several derived records for key to a single one
      // this reduced value must be the return value
      //
      // return reduced_value;
    }

The Nashorn engine supports [ECMA script 5.1](https://www.ecma-international.org/ecma-262/5.1/) including
some [custom extensions](https://wiki.openjdk.java.net/display/Nashorn/Nashorn+extensions).

## Sales-by-Country Demo

The included example code is a two-stage map-reduce-program to find the countries with the largest sales volumes.
The stages are as follows:

1. Aggregate sales by country ([script](demo_data/sales_by_country.js)).
2. Find the top 10 countries by sales volume ([script](demo_data/top10_countries.js)).

Run it as follows:

First, import the demo data is described in the first section:

    java -cp client.jar app_kvClient.demo.OnlineRetailIngest localhost:10000 demo_data/OnlineRetail.json.txt 26000

This might take a while. After it is done, start the CLI and connect to the datastore.
You can investigate the raw records, they are indexed by order ID:

    G4DB> get 536365

    GET_SUCCESS 536365 : {"invoice_no": "536365", "date": "01.12.10 08:26", "custom_no": "17850", "country": "United Kingdom", "positions": [{"product_no": "85123A", "description": "WHITE HANGING HEART T-LIGHT HOLDER", "quantity": 6, "unit_price": 255.0}, {"product_no": "71053", "description": "WHITE METAL LANTERN", "quantity": 6, "unit_price": 339.0}, {"product_no": "84406B", "description": "CREAM CUPID HEARTS COAT HANGER", "quantity": 8, "unit_price": 275.0}, {"product_no": "84029G", "description": "KNITTED UNION FLAG HOT WATER BOTTLE", "quantity": 6, "unit_price": 339.0}, {"product_no": "84029E", "description": "RED WOOLLY HOTTIE WHITE HEART.", "quantity": 6, "unit_price": 339.0}, {"product_no": "22752", "description": "SET 7 BABUSHKA NESTING BOXES", "quantity": 2, "unit_price": 765.0}, {"product_no": "21730", "description": "GLASS STAR FROSTED T-LIGHT HOLDER", "quantity": 6, "unit_price": 425.0}]}

Now let's start the first map-reduce program to aggregate sales by country and store the results in the `sales_by_country` namespace:

    G4DB> mapReduce sales_by_country demo_data/sales_by_country.js

    Started map/reduce job with id: mr1548693352013
    <mr1548693352013 (RUNNING); Workers: 1/6 (failed: 0); 1%; error: null>
    ...
    <mr1548693352013 (RUNNING); Workers: 5/6 (failed: 0); 51%; error: null>
    <mr1548693352013 (FINISHED); Workers: 6/6 (failed: 0); 100%; error: null>

You can follow the status of the job until it completes. Let's check out the result:

    G4DB> get "sales_by_country/United Kingdom"

    GET_SUCCESS sales_by_country/United Kingdom : 768787404

Next we can start the second stage to find the top 10 countries with the largest sales volumes.
We execute the second map-reduce-script on the data we just produced and write the results into the `top10_countries` namespace:

    G4DB> mapReduce sales_by_country top10_countries demo_data/top10_countries.js

    ...
    <mr1548693717263 (FINISHED); Workers: 6/6 (failed: 0); 100%; error: null>

After the job finishes, we can check the result:

    G4DB> get top10_countries/result

    GET_SUCCESS top10_countries/result : [{"country":"United Kingdom","total":768787404},{"country":"Netherlands","total":27159093},{"country":"EIRE","total":24605811},{"country":"Germany","total":19027399},{"country":"France","total":17456055},{"country":"Australia","total":13442308},{"country":"Switzerland","total":5017616},{"country":"Spain","total":4813771},{"country":"Belgium","total":3477872},{"country":"Sweden","total":3466343}]

## Limitations of the Current Implementation

- Final results are aggregated on the master which is a severe bottleneck.
  Concretely this means that we support only operations that reduce very strongly, e.g. sales by country.
  Operations with larger intermediate key sets like e.g. frequently bought item sets could be problematic.
  The better solution would be combining the final results for each key on the node
  which has to store the result eventually to distribute the load better.
  We unfortunately did not have time to do this.
- There is no fail-over mechanism for the master process (fail-over for worker processes is implemented).
