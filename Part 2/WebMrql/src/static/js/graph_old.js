// The base endpoint to receive data from. See update_url()
var URL_BASE = getURLBASE();

var xScale_Ordinal = false;
var yScale_Ordinal = false;
var ticksCount = 10;

// Update graph in response to inputs
//d3.select("#x_axis").on("input", make_graph);
//d3.select("#y_axis").on("input", make_graph);

var margin = {top: 20, right: 30, bottom: 100, left: 80};
var width = 800 - margin.left - margin.right;
var height = 600 - margin.top - margin.bottom;

// Whitespace on either side of the bars in units of minutes
var binMargin = 0.01;


// x scale
var x = d3.scale.linear()
    .range([0,  width]);
//    .domain([0, 25]);

var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
    .ticks(10);

//y scale
var y = d3.scale.linear()
    .range([height, 0]);
var yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(10);

var svg = d3.select("body").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
  .append("g")
    .attr("transform",
      "translate(" + margin.left + "," + margin.top + ")");

//For Tooltip
var div = d3.select("body").append("div")	
    .attr("class", "tooltip")				
    .style("opacity", 0);

// x axis
svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis)
    .append("text")
      .attr("class", "xaxis_label")
      .text("X-Axis")
      .attr("dy", "3em")
      .attr("text-align", "center")
      .attr("x", width / 2 - margin.right - margin.left);

// y axis
svg.append("g")
    .attr("class", "y axis")
    .call(yAxis)
  .append("text")
    .attr("class", "yaxis_label")
    .attr("transform", "rotate(-90)")
    .attr("x", -height / 2)
    .attr("dy", "-3em")
    .text("Y-Axis");

function getURLBASE(){
	var urlName = window.location.origin + window.location.pathname;
	urlName = urlName.replace("graph", "graphdata");
	return urlName;
}


// Return url to recieve data
function update_url() {
  var x_axis = document.getElementById("x_axis").value;
  var y_axis = document.getElementById("y_axis").value;

  return URL_BASE +
        "?xaxis=" + x_axis +
        "&yaxis=" + y_axis;
}

// Convert csv data to correct datatypes
function type(d) {
  //Check if xAxis data is Numerical
  if(!isNaN(d.xAxis)){
        d.xAxis = +d.xAxis;
  	xScale_Ordinal = false;
  }
  else{
    d.xAxis = d.xAxis;
    xScale_Ordinal = true;
  }

  //Check if yAxis data is Numerical
  if(!isNaN(d.yAxis)){
        d.yAxis = +d.yAxis;
  	yScale_Ordinal = false;
  }
  else{
    d.yAxis = d.yAxis;
    yScale_Ordinal = true;
  }
  return d;
}


function build_scale(){
//For String datatype, Ordinal scale needs to be used
   if (xScale_Ordinal){
       x = d3.scale.ordinal().rangeRoundBands([0, width], .05);
   }
   else{
      x = d3.scale.linear().range([0,  width]);
   }

   if (yScale_Ordinal){
      // x = d3.scale.ordinal().rangeRoundBands([0, width], .05);
       y = d3.scale.ordinal().rangeRoundBands([height, 0], .05);
   }
   else{
      y = d3.scale.linear().range([height, 0]);
   }
   //y = d3.scale.linear().range([height, 0]);
}

function make_graph() {
  var graphType = document.getElementById("graph_type").value;
  if (graphType == "line"){
	  make_scatterPlots();
  }
  else{
  	  make_barGraph();
  }
}

function make_barGraph() {
  url = update_url();
  var xaxisLabel = document.getElementById("x_axis").value;
  var yaxisLabel = document.getElementById("y_axis").value;
  d3.csv(url, type, function(error, data) {
     
     build_scale();

    
     //x = d3.scale.ordinal().rangeRoundBands([0, width], .05);
    //.range([0,  width]);

     xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
    .ticks(ticksCount);

     yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(ticksCount);

     var xMax = d3.max(data, function(d) { return d.xAxis; });

     if (xScale_Ordinal){
        x.domain(data.map(function(d) { return d.xAxis; }));
      }
      else{
	x.domain([0, d3.max(data, function(d) { return d.xAxis; })*1.2 ]);
      }

      if (yScale_Ordinal){
        y.domain(data.map(function(d) { return d.yAxis; }));
      }
      else{
	y.domain([0, d3.max(data, function(d) { return d.yAxis; })*1.2]);
      }
     
   
    //Redraw X-Axis Scale
    svg.selectAll("g.x.axis")
      .transition()
      .call(xAxis);

    //Redraw X-Axis Label
    svg.selectAll("g.x.axis")
       .select("text.xaxis_label")
       .transition()
       .text(xaxisLabel)
       .attr("dy", "3em")
       .attr("text-align", "center")
       .attr("x", width / 2 - margin.right - margin.left);

    //Redraw Y-Axis Scale
    svg.selectAll("g.y.axis")
      .transition()
      .call(yAxis);

    //Redraw Y-Axis Label
    svg.selectAll("g.y.axis")
       .select("text.yaxis_label")
       .transition()
       .attr("class", "yaxis_label")
       .attr("transform", "rotate(-90)")
       .attr("x", -height / 2)
      // .attr("y", "-9")
       .attr("dy", "-3em")
       .text(yaxisLabel);

    svg.selectAll(".mline").remove();
    svg.selectAll("circle").remove();

    var bars = svg.selectAll(".bar")
      .data(data, function(d) { return d.xAxis; });

    bars.transition(1000)
      .attr("y", function(d) { return  y(d.yAxis); } )
      .attr("height", function(d) { return height - y(d.yAxis); } );

    bars.enter().append("rect")
      .attr("class", "bar")
      .attr("x", function(d) { return x(d.xAxis); })  //-0.25
      
      if (xScale_Ordinal){
      bars.attr("width", x.rangeBand()) 
      }
      else{
        var calcWidth = xMax/(ticksCount*2);
	if (calcWidth < 2 * binMargin){
	   bars.attr("width", x(1 - 2 * binMargin)) 
	}
 	else{
	   bars.attr("width", x((xMax/(ticksCount*2)) - 2 * binMargin)) 
	}	
      }

      bars.attr("y", height)
      .attr("height", 0)
      .attr("y", function(d) { return y(d.yAxis); })

      if (yScale_Ordinal){
	 bars.attr("height", function(d) { return y.rangeBand(); })
      }
      else{
	 bars.attr("height", function(d) { return height - y(d.yAxis); })
      }
      //.attr("height", function(d) { return height - y(d.yAxis); })
      //.attr("height", function(d) { return y.rangeBand(); })
      
      bars.on("mouseover", function(d) {		
            div.transition()		
                .duration(200)		
                .style("opacity", .9);		
            div.html(xaxisLabel+":"+d.xAxis + "<br/>"  + yaxisLabel+":"+d.yAxis)	
                .style("left", (d3.event.pageX) + "px")		
                .style("top", (d3.event.pageY - 28) + "px");	
            })					
        .on("mouseout", function(d) {		
            div.transition()		
                .duration(500)		
                .style("opacity", 0);	
        });


    bars.exit()
      .transition(1000)
        .attr("y", height)
        .attr("height", 0)
      .remove();
  });
}

function make_scatterPlots(){
    url = update_url();
    var xaxisLabel = document.getElementById("x_axis").value;
    var yaxisLabel = document.getElementById("y_axis").value;

    // Get the data
    d3.csv(url, type, function(error, data) {
      
     build_scale(); 

     xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
    .ticks(ticksCount);

     yAxis = d3.svg.axis()
    .scale(y)
    .orient("left")
    .ticks(ticksCount);

    // Scale the range of the data
    if (xScale_Ordinal){
        x.domain(data.map(function(d) { return d.xAxis; }));
     }
    else{
	x.domain([0, d3.max(data, function(d) { return d.xAxis; })*1.2 ]);
     }

    // x.domain(d3.extent(data, function(d) { return d.xAxis; }));
    //y.domain([0, d3.max(data, function(d) { return d.yAxis; })*1.2 ]);
    if (yScale_Ordinal){
        y.domain(data.map(function(d) { return d.yAxis; }));
     }
    else{
        y.domain([0, d3.max(data, function(d) { return d.yAxis; })*1.2 ]);
     }

    //Redraw X-Axis Scale
    svg.selectAll("g.x.axis")
      .transition()
      .call(xAxis);

    //Redraw X-Axis Label
    svg.selectAll("g.x.axis")
       .select("text.xaxis_label")
       .transition()
       .text(xaxisLabel)
       .attr("dy", "3em")
       .attr("text-align", "center")
       .attr("x", width / 2 - margin.right - margin.left);

    //Redraw Y-Axis Scale
    svg.selectAll("g.y.axis")
      .transition()
      .call(yAxis);

    //Redraw Y-Axis Label
    svg.selectAll("g.y.axis")
       .select("text.yaxis_label")
       .transition()
       .attr("class", "yaxis_label")
       .attr("transform", "rotate(-90)")
       .attr("x", -height / 2)
      // .attr("y", "-9")
       .attr("dy", "-3em")
       .text(yaxisLabel);

  //  var chart = svg.selectAll("g.y.axis");
    
    //Calculate X-Axis Offset for String Value Ploting
    var xoffset = 0;
    if (xScale_Ordinal){
	var maxVal = d3.max(data, function(d){return x(d.xAxis)});
	var minVal = d3.min(data, function(d){return x(d.xAxis)});
	var valArray = d3.values(data, function(d){return x(d.xAxis)});
	varArray = valArray.length - 1;
    	xoffset = (maxVal - minVal)/(varArray*2); //42.5 
    }		
    //Calculate Y-Axis Offset for String Value Ploting
    var yoffset = 0;
    if (yScale_Ordinal){
        var maxVal = d3.max(data, function(d){return y(d.yAxis)});
        var minVal = d3.min(data, function(d){return y(d.yAxis)});
        var valSize = d3.values(data, function(d){return y(d.yAxis)});
        valSize = valSize.length - 1;
        yoffset = (maxVal - minVal)/(valSize*2);
    }


    // Define the line
    var valueline = d3.svg.line()
		          .x(function(d) { return x(d.xAxis)+xoffset; })
		          .y(function(d) { return y(d.yAxis)+yoffset; });    
        
   	svg.selectAll(".bar").remove();
	svg.selectAll(".mline").remove();
	svg.selectAll("circle").remove();
		
    var chart = svg.selectAll(".mline")
				   .data("1"); //, function(d) { return d.xAxis; });

    // Add the valueline path.
   // svg.selectAll(".mline")
	//    .data("1")
	    chart.enter()
		.append("path")
        .attr("class", "mline");
		
		if (!xScale_Ordinal){
          chart.attr("d", valueline(data.sort(function(a,b) { return d3.ascending(a.xAxis,b.xAxis) })));
		}
		else{
		  chart.attr("d", valueline(data));	
		}
		
	// chart.exit().remove();
      //.transition(1000)
    //  .attr("d", valueline(data.sort(function(a,b) { return d3.ascending(a.xAxis,b.xAxis) })))
 //     .remove(); 

	
    // Add the scatterplot
    svg.selectAll("circle")
        .data(data)
      .enter().append("circle")
        .attr("r", 3.5)
        .attr("cx", function(d) { return x(d.xAxis)+xoffset; })
        .attr("cy", function(d) { return y(d.yAxis)+yoffset; })
	    .on("mouseover", function(d) {		
            div.transition()		
                .duration(200)		
                .style("opacity", .9);		
            div.html(xaxisLabel+":"+d.xAxis + "<br/>"  + yaxisLabel+":"+d.yAxis)	
                .style("left", (d3.event.pageX) + "px")		
                .style("top", (d3.event.pageY - 28) + "px");	
            })					
        .on("mouseout", function(d) {		
            div.transition()		
                .duration(500)		
                .style("opacity", 0);	
        });
	
   

    });
}

make_graph();
