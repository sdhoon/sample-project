 <html>
 <head>
 <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
 <title>Insert title here</title>
 <script src="https://code.jquery.com/jquery-3.0.0.min.js"></script>
 <script src="https://code.jquery.com/ui/1.11.4/jquery-ui.js"></script>
 <link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css">
 
 <style>

 
 ul {
    list-style:none;
    margin:0;
    padding:0;
}

li {
    margin: 0 0 0 0;
    padding: 0 0 0 0;
    border: solid 1px gray;
    border-bottom: 0;
    float: left;
}
 
 </style>
 
 <script type="text/javascript">
 var host = "http://" + window.location.host;
 
 	$(function() {
 		$("#search").click(function(){
 			var itemId = $("#itemid").val();
 			getRecommend(itemId);
 			getAssociation(itemId);
 			//getBrand(itemId);
 			
 		});
 	});
 	
 	function getBrand(itemId){
 		$("#itemid").val(itemId);
 		var url = host + "/brandbest";
 		var params = "id=" + itemId;
 		
 		$.ajax({
			url : url, 
			data : params,
			dataType : 'json', 
			success: function(data){
				$("#brand").html('');
				
				var brandId = data.brandId;
				var brandNm = data.brandNm;
				var brandBestItemIds = data.brandBestItemIds;
				
				
				$("#brandTitle").html("[" + brandNm + "]")
				
				var brand = '';
				var brandNum = 0;
				$.each(brandBestItemIds, function(i, item){
					
					//if(item.lift >= 1) {
						
						if(brandNum % 10 == 0){
							brand += "<div class='row'>"
						}
						
						brand += "<div class='col-lg-2'>";
						brand += "<p align='center'><img src='http://img.ssgcdn.com/trans.ssg?src=" + item.itemId + "_i1.jpg&w=150&h=150'></p>";
						brand += "<p align='center'><a href=javascript:getRecommend('"+ item.itemId + "')>[ì¡°í]</a>&nbsp;"
						brand += "<a href='http://emart.ssg.com/item/itemView.ssg?itemId=" + item.itemId + "' target='_blank'>[ìíìì¸]</a></p>"
						brand += "</div>";
						
						if(brandNum % 10 == 9){
							brand += "</div>";
						}
						
						brandNum++;
						
					//}
					
					
					
				});
				
				$("#brand").append(brand);
				
			},
			error: function(e){
				alert(e.responseText);
			}
		})
 		
 		
 		
 	}
 	
 	function getAssociation(itemId){
 		$("#itemid").val(itemId);
 		var url = host + "/association/" + itemId;
		//var params = "id=" + itemId;
 		
		$.ajax({
			url : url, 
			//data : params,
			dataType : 'json', 
			success: function(data){
				$("#association").html('');
				
				var itemId = data.itemId;
				var associationConsequent = data.associationConsequents;
				
				var association = '';
				var associationNum = 0;
				$.each(data, function(i, item){
					
					//if(item.lift >= 1) {
						
						if(associationNum % 10 == 0){
							association += "<div class='row'>"
						}
						
						association += "<div class='col-lg-2'>";
						association += "<p align='center'><img src='http://img.ssgcdn.com/trans.ssg?src=" + item.consequent + "_i1.jpg&w=150&h=150'></p>";
						association += "<p align='center'>[support : " + item.support + "]</p>";
						association += "<p align='center'>[confidence : " + item.confidence + "]</p>";
						association += "<p align='center'>[lift : " + item.lift + "]</p>";
						association += "<p align='center'><a href=javascript:getRecommend('"+ item.consequent + "')>[조회]</a>&nbsp;"
						association += "<a href='http://emart.ssg.com/item/itemView.ssg?itemId=" + item.consequent + "' target='_blank'>[상품상세]</a></p>"
						association += "</div>";
						
						if(associationNum % 10 == 9){
							association += "</div>";
						}
						
						associationNum++;
						
					//}
					
					
					
				});
				
				$("#association").append(association);
				
			},
			error: function(e){
				alert(e.responseText);
			}
		})
 		
 	}
 	
 	function getRecommend(itemId){
 			$("#itemid").val(itemId);
			var url = host + "/v1.0/itembase/" + itemId + "/siteno/6001"
			
			
			$.ajax({
			url : url,			
			dataType : 'json', 
			success: function(data){
				$("#listLayout").html(''); 				
				
				var recommendItemId = data.recommendItemIds;
				
				
				var link = "<a href='http://emart.ssg.com/item/itemView.ssg?itemId=" + itemId + "' target='_blank'><img src='http://img.ssgcdn.com/trans.ssg?src=" + itemId + "_i1.jpg&w=300&h=300'></a>";
				link += "<p>" + data.stdCtg + "</p>";
				$("#listLayout").append(link);
				
				
				
				$("#click").html('');
				$("#order").html('');
				
				var click="";
				var order="";
				var clickNum = 0;
				var orderNum = 0;
				$.each(data, function(i, item){
					
				//	if(item.method=="click"){
						
						if(clickNum % 10 == 0){
							click += "<div class='row'>"
						}
						
						click += "<div class='col-lg-1'>";
						click += "<p align='center'><img src='http://img.ssgcdn.com/trans.ssg?src=" + item.itemID + "_i1.jpg&w=150&h=150'></p>";
						//click += "<p align='center'>" + item.score + "</p>";
						click += "<p align='center'>[" + item.recommendStdCtg + "]</p>";
						click += "<p align='center'><a href=javascript:getRecommend('"+ item.itemID + "')>[조회]</a>&nbsp;"
						click += "<a href='http://emart.ssg.com/item/itemView.ssg?itemId=" + item.itemID + "' target='_blank'>[상품상세]</a></p>"
						click += "</div>";
						
						if(clickNum % 10 == 9){
							click += "</div>";
						}
						
						clickNum++;
						
					/*} else if(item.method=="order"){
						
						if(orderNum % 10 == 0){
							order += "<div class='row'>"
						}
						
						order += "<div class='col-lg-1'>";
						order += "<p align='center'><img src='http://img.ssgcdn.com/trans.ssg?src=" + item.itemid + "_i1.jpg&w=200&h=200'></p>";
						order += "<p align='center'>" + item.score + "</p>";
						order += "<p align='center'>[" + item.recommendStdCtg + "]</p>";
						order += "<p align='center'><a href=javascript:getRecommend('"+ item.itemid + "')>[ì¡°í]</a>&nbsp;"
						order += "<a href='http://emart.ssg.com/item/itemView.ssg?itemId=" + item.itemid + "' target='_blank'>[ìíìì¸]</a></p>"
						order += "</div>";
						
						if(orderNum % 10 == 9){
							order += "</div>";	
							
						}
						
						orderNum++;
					} */
				});
				
				$("#click").append(click);
				$("#order").append(order);
				
				
			},
			error: function(e){
				alert(e.responseText);
			}
		})
	}
 </script>
 </head>
 
 <body>
  <input type="text" id="itemid">
  <input type="button" id="search" value="조회">
  
  
  	<div id="listLayout"></div>
  	
  	
  	<div class="row">
  		<div class='col-lg-5'><h1 style='color:blue'>자주 같이 사는 상품</h1></div>
	</div>
		
  	
  	<div id="association"></div>
  
  
  	<div class="row">
  		<div class='col-lg-5'><h1 style='color:blue'>남들은 어떤 상품을 쓱 봤을까?</h1></div>
	</div>
		
  	
  	<div id="click"></div>
  	
  	
  	<div class="row">
  		<div class='col-lg-5'><h1 style='color:blue'>브랜드 상품</h1></div>
  		<div id='brandTitle' ><h1 style='color:blue'></h1></div>
  		
	</div>
	
	<div id="brand"></div>
  	
  
 		
 	
	
  	
  
 </body>
 </html>
 Request path is ${context.request().path()}
 
 
 