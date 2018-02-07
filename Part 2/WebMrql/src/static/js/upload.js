function clearQuery(tagID){
	var qry = document.getElementById(tagID);
	qry.value = "";
}
		
function disableMenu(option,menuID)
{
	var id = document.getElementById(menuID)
	if (option == 'distributed'){
		id.disabled = false;
	}
	else{
		id.disabled = true;
	//	id.value = "1";
	}
	$('.selectpicker').selectpicker('refresh'); 
}

window.addEventListener("load",function(){
		disableMenu(document.getElementById('run_modeid').value, 'nodes_menu');
},false);
		
		
var refresh_id = "";
var counter = 0;
$(document).ready(checkForDistMode);

function checkThread() {
	counter++;
	var checkThreadURL = window.location.pathname + window.location.search;
	checkThreadURL = checkThreadURL.replace("upload", "checkThread");
	$.get(
		  checkThreadURL,
		  function(data) {
		  console.log(data);
	        if (data == 'thread_finished') {
	           clearInterval(refresh_id);
	           document.getElementById("loadPrevSession").click();
	        }
	        else{
	        	if (data == 'no_thread') {
		           clearInterval(refresh_id);
	        	}
	        }
		  }
		  );
    if (counter >= 100)
    {
        clearInterval(refresh_id);
    }	
}

function checkForDistMode() {
	counter = 0;
	var mode = document.getElementById('run_modeid').value;
	var query = document.getElementById('query_id').value;
	if (mode == 'distributed' && query != ''){
		refresh_id = setInterval(checkThread, 5000); // 5 Seconds Interval
	}
}
