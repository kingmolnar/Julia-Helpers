#!/usr/bin/env node
/*
	extractobjects.js reads the position and size of sensor and zone objects
	from a drawing in SVG. The output is a CSV file of the form

	"object","id","color","x","y","width","height"
	"sensor","01","#ff0001","256.38971","499.97635","5.2184544","5.2184544"
	"sensor","00","#ff0000","352.18457","500.10065","5.2184544","5.2184544"
	"zone","00","#00ff00","189.24809","313.95605","699.33331","238"


*/
var request = require('request');
var cheerio = require('cheerio');
var fs = require('fs');
var stringify = require('csv-stringify');

function work(err, resp, body) {
    if (err)
        throw err;
	//console.log(body.toString())
    $ = cheerio.load(body.toString());

	var results = [];
	results.push(["object", "objectid", "color", "x", "y", "width", "height"])
	$("rect").each(function () {
		var stystring = $(this).attr("style");
		var width = $(this).attr("width");
		var height = $(this).attr("height");
		var x = $(this).attr("x");
		var y = $(this).attr("y");
		var transform = $(this).attr("transform");
		if (transform == "scale(1,-1)") {
			y = -y
		}

        if (stystring) {
    		var stys = stystring.split(";")
    		//console.log(stys)
    		var style = {}
    		for (var s in stys) {
    			//console.log(stys[s])
    			var ss = stys[s].split(":")
    			style[ss[0]] = ss[1]
    		}

    		var id = style["stroke"].substr(5, 7)
    		var typ
    		//console.log(style["stroke"].substr(1, 4))
    		switch(style["stroke"].substr(1, 4)) {
    		case "ff00":
    			typ = "sensor"
    			break
    	    case "0ff0":
    			typ = "camera"
    			break
    		case "00ff":
    			typ = "zone"
    			break
            case "00f8":
    			typ = "marker"
    			break
            case "8888":
    			typ = "pedsim"
    			break
    		case "4000":
    			typ = "scale_ft"
    			break
            case "4010":
    			typ = "scale_x_ft"
    			break
            case "4020":
    			typ = "scale_y_ft"
    			break
            case "4008":
    			typ = "scale"
    			break
            case "4008":
    			typ = "scale"
    			break
            case "4018":
    			typ = "scale_x"
    			break
	        case "4028":
    			typ = "scale_y"
    			break
            case "0000":
    			typ = "frame"
    			break
    		default:
    			typ = "unknown"
    			break
    		}
    		results.push( [ typ, id, style["stroke"], x, y, width, height] );
        } else {
            //console.log($(this))
        }
	})
	//console.log("records counted: "+results.length)

    $("path").each(function () {
        var stystring = $(this).attr("style");
        var style_stroke = /stroke:#([0-9a-fA-F]{2})00ff/.exec(stystring)
        var d = $(this).attr("d");
        var id = $(this).attr("id");
        var label = $(this).attr("inkscape:label");
        /*var transform = $(this).attr("transform");
        if (transform == "scale(1,-1)") {
			y = -y
		}*/
        //var stys = stystring.split(";")
        if (style_stroke && style_stroke.length>=2) {
            //console.log(style_stroke
            var line = /([mM])\s+([\-\d.]+),([\-\d.]+)\s+([\-\d.]+),([\-\d.]+)/.exec(d); //,(\-[\d\.]+)\s+([\-\d\.]+),(\-[\d\.]+)/.exec(d)
            if (line && line.length>5) {
               // console.log(line)
                if (line[1]=='m') {
                    results.push( ["line", style_stroke[1], style_stroke[1]+"00ff", line[2], line[3], line[4], line[5] ] )
                } else {
                    results.push( ["line", style_stroke[1], style_stroke[1]+"00ff", line[2], line[3], 1.0*line[2]+line[4], 1.0*line[3]+line[5] ] )
                }

            }
            //console.log(style_stroke[1]+"  "+d)
        }
		/*var style = {}
		for (var s in stys) {
			console.log(stys[s])
			var ss = stys[s].split(":")
			style[ss[0]] = ss[1]
		}*/



    })

	var stringifier = stringify({delimiter: ',', quoted:true})
	var data = ''
	stringifier.on('readable', function(){
	  while(row = stringifier.read()){
	    data += row;
	  }
	});
	stringifier.on('error', function(err){
	  consol.log(err.message);
	});
	stringifier.on('finish', function(){
	});

	results.forEach( function (row) {
		stringifier.write(row);
	});
	stringifier.end();

	console.log(data);

    // TODO: scraping goes here!
}


var url = process.argv[2]
//console.log("Loading URL `"+url+"'")
//request(url, work);
fs.readFile(url, function(err, body) { work(err, null, body); } );
