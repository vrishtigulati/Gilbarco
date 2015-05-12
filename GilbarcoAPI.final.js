var express = require('express');
var usergrid = require('usergrid');
//var bodyparser = require('body-parser');
var async = require('async');
var _=require('underscore');

// Set up Express environment and enable it to read and write JavaScript
var app = express();
app.use(express.bodyParser());
//app.use(bodyparser.json());

// Initialize Usergrid
var client = new usergrid.client({
	'orgName' : 'insights-demo-2-0',
	'appName' : 'gilbarco',
	'URI':'https://api-connectors-prod.apigee.net/appservices',
	'clientId' : 'b3U6UBCSavNkEeS0j8t9pw-sUw',
	'clientSecret' : 'b3U6f_tgPRAlH3rrHQKo1VnCXqAgSRY',
	'authType' : usergrid.AUTH_CLIENT_ID,
});


// The API starts here

// GET /
app.get('/', function(req, res){

  //res.send("index - refer documentation");
  res.jsonp(200, 
           {'Usage Guidelines' :[]}
			);                        
});
// GET /profiles

app.get('/locations', function(req, res) {	
		getLocations(req, res);
});

function getLocations(req, res) {
	client.createCollection({
		type : 'locations'
	}, function(err, locations) {
		if (err) {
			console.log("Error : " + JSON.stringify(err));
			res.jsonp(500, {
				'error' : JSON.stringify(err)
			});
			return;
		}

		var locs = [];
		while (locations.hasNextEntity()) {
			var loc = locations.getNextEntity().get();
			var e = {
				"Location" : loc.Location,
				"DisplayName": loc.LocationName + '-' + loc.stationZip
			};
			locs.push(e);
		}
		res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Content-Type", "application/json");
		res.jsonp(200,locs);
	});
}

app.get('/weathertypes', function(req, res) {	
		getWeatherTypes(req, res);
});

function getWeatherTypes(req, res) {
	client.createCollection({
		type : 'weathertypes'
	}, function(err, WeatherTypes) {
		if (err) {
			console.log("Error : " + JSON.stringify(err));
			res.jsonp(500, {
				'error' : JSON.stringify(err)
			});
			return;
		}

		var weathers = [];
		while (WeatherTypes.hasNextEntity()) {
			var wt = WeatherTypes.getNextEntity().get();
			weathers.push(wt.name);
		}
		res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Content-Type", "application/json");
		res.jsonp(200,weathers);
	});
}

app.get('/products', function(req, res) {	
		getProducts(req, res);
});

function getProducts(req, res) {
	client.createCollection({
		type : 'ProductDemos'
	}, function(err, Products) {
		if (err) {
			console.log("Error : " + JSON.stringify(err));
			res.jsonp(500, {
				'error' : JSON.stringify(err)
			});
			return;
		}

		var prods = [];
		while (Products.hasNextEntity()) {
			var prod = Products.getNextEntity().get();
			var p = {
				"ProductID": prod.ProductID,
				"ProductName": prod.ProductName,
				"ProductPrice111": prod.ProductPrice
			};
			prods.push(p);
		}
		res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Content-Type", "application/json");
		res.jsonp(200,prods);
	});
}
app.get('/recommendations', function(req, res) {
	
	// Prepare Keys
	var predictionKeys = [];
	var weatherData = req.query.weather;
	if (weatherData) {
		predictionKeys.push(weatherData);
	}
	
	var l = req.query.location;
	var t = req.query.time;
	if(l && t){
		var tArr = t.toString().split(":");  //just the hr
		var locTimeData = l+'-'+tArr[0]+':';
		predictionKeys.push(locTimeData);
	}
		
	var prodsData = req.query.productID;
	if (prodsData){
		predictionKeys = predictionKeys.concat(prodsData);
	}

	//Query BaaS for each predictionKey and get results (top 10 for each).
	async.map(predictionKeys, getRecommendationsPerKey, function(err, results){
	
	
	//Merge results into 1 big array
	var mergeResults = [];
	for (key in results) {
			for (innerkey in results[key]) {
			mergeResults.push(results[key][innerkey]);   
		}
	}

 	//Sort results
	mergeResults.sort(function (a, b) {
		return parseFloat(b.Score) - parseFloat(a.Score);
	});
      
    //remove duplicates
    mergeResults =_.uniq(mergeResults, function(item, ProductID){return item.ProductID;});
    
	
    //remove results which were "inputs" to the recommendations API
    if(prodsData)
    {
      var filteredresults = _.reject(mergeResults, function(item){ 
          
		  if (prodsData instanceof Array) {
			for(var i = 0; i < prodsData.length; i++){
				if	(prodsData[i] == item.ProductID)
					return true;
			}
			return false;
		  } else {
		  if (prodsData == item.ProductID)
			return true;
		  else
			return false;
		  }
      });
      
      mergeResults = filteredresults;
    }
	     
	//share top N results
	var topN = req.query.topN;
	if (!topN || topN <= 0)
		topN = 3;   //Default to 3
	
	if(topN > mergeResults.length)
		topN = mergeResults.length;
	
	var shareResults = [];
	for (i=0; i<topN; i++) {
	shareResults.push(mergeResults[i]);
	}

		res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Content-Type", "application/json");
		res.jsonp(200,shareResults);	
	});
});

function getRecommendationsPerKey(predictionKey, callback) {
	
	var qsData = {ql:"PredictionKey='"+predictionKey+"' order by Score desc"}
	var options = {
            endpoint:"/propensities",
			qs:qsData
        };

	client.request(options, function (err, result) {
		if (err) {
			console.log("Error : " + JSON.stringify(err));
			callback(err);
		}
		var queryResults = result.entities;
		var recommendationsToReturn = [];
		
		for (var i = 0, len = queryResults.length; i < len; i++) {
		var productItemToReturn = 
			{	"ProductID": queryResults[i].ProductID,
				"ProductName": queryResults[i].ProductName,
				"Score":queryResults[i].Score,
				"PredictionKey":queryResults[i].PredictionKey
            }
			recommendationsToReturn.push(productItemToReturn);
		}
		callback(err, recommendationsToReturn);
	});
}


// Listen for requests until the server is stopped
app.listen(process.env.PORT || 9000);
console.log('The server is running!');

