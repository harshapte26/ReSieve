
var up_count;

const urlSearchParams = new URLSearchParams(window.location.search);
const params = Object.fromEntries(urlSearchParams.entries());
console.log("query Params ====>>", params['code'])

function searchFunc(){
    console.log("in search func")
    // main_feed_cont = document.getElementById("main_feed")
    // main_feed_cont.innerHTML = "";
    $(".container").html(""); 
    var search_query = document.getElementById("search-query").value;
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "POST", "https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/postSearch", false ); // false for synchronous request
    xmlHttp.setRequestHeader("Access-Control-Allow-Origin" ,"*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Methods", "*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
    xmlHttp.send(search_query);
    res1 = xmlHttp.responseText;
    // console.log("RES1 --->>>", res1);
    res2 = JSON.parse(res1);
    // console.log("RES 2 in HTTP GET FUNCC---->", res2);
    res2 = res2['a'];
    // console.log("RES 2 Updated---->", res2);

    var upCounter = 1;
    res2.forEach((element)=>{
    var upCountId = 'counter_'+String(upCounter)

    var user_post_id = element["user_id"]
    var name_post = element["post_title"];
    var text_to_display = element["content_text"];
    var post_tag = element["post_tag"];
    var up_count = element["upvotes"];
    var stamp = element["timestamp"];
    var image_path = "https://resieve-image.s3.amazonaws.com/"+String(element["content_image"]).trim();
    console.log("IMAGE PATH S3=====>>", image_path)

    if (element["content_image"] == " Null" || element["content_image"] == "Null"){
        var newtext = `<div class="subforum1" id="main_feed">
            <div class="subforum-title">
                <h1>${name_post}</h1>
            </div>
            <div class="subforum-row">
                <div class="subforum-icon subforum-column center">
                    <i class="fa fa-user-circle center"></i>
                </div>
                <div class="subforum-description subforum-column">
                    <h4><a href="#">${post_tag}</a></h4>
                    <p>${text_to_display}</p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter("${upCountId}")">Upvote</button>
                    <p id="${upCountId}" >Upvotes : "${up_count}"</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b> Posted by ${user_post_id} on </b>
                    <br>on <small>${stamp}</small>
                </div>
            </div>
        </div>`
    $(".container").append(newtext);
    }

    else{
        console.log("Upcount in else --->>", up_count)
        console.log("Upcount datatype --->", typeof up_count)
        var new_img_text = `<div class="subforum1" id="main_feed">
            <div class="subforum-title">
                <h1>${name_post}</h1>
            </div>
            <div class="subforum-row">
                <div class="subforum-icon subforum-column center">
                    <i class="fa fa-user-circle center"></i>
                </div>
                <div class="subforum-description subforum-column">
                    <h4><a href="#">${post_tag}</a></h4>
                    <p><img src=${image_path} alt=${text_to_display} width=300 height=300></p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter(${upCountId})">Upvote</button>
                    <p id="${upCountId}" >Upvotes : "${up_count}"</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b> Posted by ${user_post_id} on </b>
                    <br>on <small>${stamp}</small>
                </div>
            </div>
        </div>`

    $(".container").append(new_img_text);
    }
    // console.log(res2);

    // upCounter++;
    upCounter = upCounter+1;
    });
}

function httpGet(theUrl)
{
    // return false;
    var clean_container = document.getElementById("main_feed");
    if (typeof(clean_container) != 'undefined' && clean_container != null)
    {
        clean_container.innerHTML = " ";
    }
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", theUrl, false ); // false for synchronous request
    xmlHttp.setRequestHeader("Access-Control-Allow-Origin" ,"*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Methods", "*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
    xmlHttp.send("hello");
    res1 = xmlHttp.responseText;
    // console.log("RES1 --->>>", res1);
    res2 = JSON.parse(res1);
    console.log("RES 2 in HTTP GET FUNCC---->", res2);
    res2 = res2['a'];
    console.log("RES 2 Updated---->", res2);

    var upCounter = 1;
    res2.forEach((element)=>{
    var upCountId = 'counter_'+String(upCounter)

    var user_post_id = element["user_id"];
    var name_post = element["post_title"];
    var text_to_display = element["content_text"];
    var post_tag = element["post_tag"];
    var up_count = element["upvotes"];
    var stamp = element["timestamp"];
    var image_path = "https://resieve-image.s3.amazonaws.com/"+String(element["content_image"]).trim();
    console.log("IMAGE PATH S3=====>>", image_path)

    if (element["content_image"] == " Null" || element["content_image"] == "Null"){
        var newtext = `<div class="subforum1" id="main_feed">
            <div class="subforum-title">
                <h1>${name_post}</h1>
            </div>
            <div class="subforum-row">
                <div class="subforum-icon subforum-column center">
                    <i class="fa fa-user-circle center"></i>
                </div>
                <div class="subforum-description subforum-column">
                    <h4><a href="#">${post_tag}</a></h4>
                    <p>${text_to_display}</p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter("${upCountId}")">Upvote</button>
                    <p id="${upCountId}" >Upvotes : "${up_count}"</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b> Posted by ${user_post_id} on </b>
                    <br>on <small>${stamp}</small>
                </div>
            </div>
        </div>`
    $(".container").append(newtext);
    }

    else{
        console.log("Upcount in else --->>", up_count)
        console.log("Upcount datatype --->", typeof up_count)
        var new_img_text = `<div class="subforum1" id="main_feed">
            <div class="subforum-title">
                <h1>${name_post}</h1>
            </div>
            <div class="subforum-row">
                <div class="subforum-icon subforum-column center">
                    <i class="fa fa-user-circle center"></i>
                </div>
                <div class="subforum-description subforum-column">
                    <h4><a href="#">${post_tag}</a></h4>
                    <p><img src=${image_path} alt=${text_to_display} width=300 height=300></p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter(${upCountId})">Upvote</button>
                    <p id="${upCountId}" >Upvotes : "${up_count}"</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b> Posted by ${user_post_id} on </b>
                    <br>on <small>${stamp}</small>
                </div>
            </div>
        </div>`

    $(".container").append(new_img_text);
    }
    // console.log(res2);

    // upCounter++;
    upCounter = upCounter+1;
    });


    
}

function httpGetOnSubmit(theUrl)
{
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "POST", theUrl, false ); // false for synchronous request
    xmlHttp.setRequestHeader("Access-Control-Allow-Origin" ,"*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Methods", "*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
    xmlHttp.send("hello");
    res1 = xmlHttp.responseText;
    // console.log("RES1 --->>>", res1);
    res2 = JSON.parse(res1);
    // console.log("RES 2 in HTTP GET FUNCC---->", res2);
    res2 = res2['a'];
    // console.log("RES 2 Updated---->", res2);

    var upCounter = 1
    res2.forEach((element, index)=>{
    var upCountId = 'counter_'+String(upCounter)

    var name_post = element["post_title"];
    var text_to_display = element["content_text"];
    var post_tag = element["post_tag"];
    var up_count = element["upvotes"];
    var stamp = element["timestamp"]

    var newtext = `<div class="subforum1">
            <div class="subforum-title">
                <h1>${name_post}</h1>
            </div>
            <div class="subforum-row">
                <div class="subforum-icon subforum-column center">
                    <i class="fa fa-car center"></i>
                </div>
                <div class="subforum-description subforum-column">
                    <h4><a href="#">${post_tag}</a></h4>
                    <p>${text_to_display}</p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter("${upCountId}")">Upvote</button>
                    <p id="${upCountId}" >Upvotes : "${up_count}"</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b> Posted by User on </b>
                    <br>on <small>${stamp}</small>
                </div>
            </div>
        </div>`

    $(".container").append(newtext);
    console.log(res2);

    upCounter++;
    });


    
}

function update_counter(inelement){
    console.log("inelement --->>", inelement)
    new_id = inelement.id
    inner_val = document.getElementById(new_id).innerHTML;
    console.log("iner parsing", inner_val[(inner_val.length)-1])
    var mySubString = inner_val.substring(
    inner_val.indexOf('"') + 1, 
    inner_val.lastIndexOf('"')
);
    console.log("mysbsyt",mySubString)
    new_count = parseInt(mySubString)
    document.getElementById(new_id).innerHTML = "Upvotes:" + `"${++new_count}"`;
}

// function upload_image(){
//     // var file = document.getElementById('file_path').files[0];
//     var file_name = document.getElementById("myfile").mozFullPath;
//     console.log("file_name =====>> ", file_name)
// }

function upload_image(){
// var tag_image = document.getElementById('post_tag').value;
var file = document.getElementById('myfile').files[0];
var type_file_up = file.type;
var name_f = file.name;
console.log("File Type ===>", type_file_up)

var d = new Date();
d = new Date(d.getTime() - 3000000);
var date_format_str = d.getFullYear().toString()+"-"+((d.getMonth()+1).toString().length==2?(d.getMonth()+1).toString():"0"+(d.getMonth()+1).toString())+"-"+(d.getDate().toString().length==2?d.getDate().toString():"0"+d.getDate().toString())+" "+(d.getHours().toString().length==2?d.getHours().toString():"0"+d.getHours().toString())+":"+((parseInt(d.getMinutes()/5)*5).toString().length==2?(parseInt(d.getMinutes()/5)*5).toString():"0"+(parseInt(d.getMinutes()/5)*5).toString())+":00";

var name = document.getElementById("post_title").value;
var text_to_display = document.getElementById("newthread").value;
var post_tag = document.getElementById("post_tag").value;
var upvotes = 0;
var user_id = 'user_1';
var content_text = 'Null';
var timestamp = date_format_str

// ingest_data = {
// 'post_id':String(parseInt(Math.random()*1000)),
// 'user_id': String(user_id),
// 'post_title': String(name),
// 'post_tag': String(post_tag),
// 'content_text': String(content_text),
// 'content_image': String(content_text),
// 'upvotes': String(upvotes),
// 'timestamp': String(date_format_str)
// }


// var file = document.querySelector(
//         'input[type=file]')['files'][0];

var base64String = "";
var reader = new FileReader();
console.log("next");
    
reader.onload = function () {
    base64String = reader.result.replace("data:", "")
        .replace(/^.+,/, "");

    imageBase64Stringsep = base64String;

    // alert(imageBase64Stringsep);
    // console.log(base64String);
    ingest_data = {
    'post_id':String(parseInt(Math.random()*1000)),
    'user_id': String(user_id),
    'post_title': String(name),
    'post_tag': String(post_tag),
    'content_text': String(content_text),
    'content_image': base64String,
    'upvotes': String(upvotes),
    'timestamp': String(timestamp)
    }

    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "POST", "https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/dataUpload", false ); // false for synchronous request
    xmlHttp.setRequestHeader("x-amz-meta-customLabels" ,post_tag);
    xmlHttp.setRequestHeader("Content-Type", type_file_up);
    xmlHttp.setRequestHeader("filename", name_f)
    // xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");

    xmlHttp.send(JSON.stringify(ingest_data));
    res1 = xmlHttp.responseText;
    res2 = JSON.parse(res1);
    console.log("RESPONSE ----->>>>>>>", res2)
    
    if(res2['isValid'] === 1){
        console.log("ITS APRROVED!!!!!")
        alert("Image Uploaded Successfully")
    }
    else{
        alert("WARNING!!!\n Image modified according to community standards as offensive content detected in the image")
    }
}
reader.readAsDataURL(file);

location.reload()
}


function send_text(){

    var d = new Date();
    d = new Date(d.getTime() - 3000000);
    var date_format_str = d.getFullYear().toString()+"-"+((d.getMonth()+1).toString().length==2?(d.getMonth()+1).toString():"0"+(d.getMonth()+1).toString())+"-"+(d.getDate().toString().length==2?d.getDate().toString():"0"+d.getDate().toString())+" "+(d.getHours().toString().length==2?d.getHours().toString():"0"+d.getHours().toString())+":"+((parseInt(d.getMinutes()/5)*5).toString().length==2?(parseInt(d.getMinutes()/5)*5).toString():"0"+(parseInt(d.getMinutes()/5)*5).toString())+":00";

    var name = document.getElementById("post_title").value;
    var text_to_display = document.getElementById("newthread").value;
    var post_tag = document.getElementById("post_tag").value;
    var upvotes = 0;
    var user_id = 'user_1';
    var content_image = 'Null';
    var timestamp = date_format_str

    ingest_data = {
    'post_id':String(parseInt(Math.random()*1000)),
    'user_id': String(user_id),
    'post_title': String(name),
    'post_tag': String(post_tag),
    'content_text': String(text_to_display),
    'content_image': String(content_image),
    'upvotes': String(upvotes),
    'timestamp': String(date_format_str)
    }

    // var up_count = 24

    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "POST", "https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/textUpload", false ); // false for synchronous request
    xmlHttp.setRequestHeader("Access-Control-Allow-Origin" ,"*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Methods", "*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
    xmlHttp.send(JSON.stringify(ingest_data));
    res1 = xmlHttp.responseText;
    res2 = JSON.parse(res1);

    // console.log("res 2 in send text ====>", res2)

    if (res2['isValid'] === 1){
        var offense_labels = res2['detect_labels'];
        alert_string = "Offensive content detected!!!\n Post content may have the following categories: " + offense_labels.toString();
        alert(alert_string)
    }
    else{
        console.log("HERE IN ELSE SEND TEXT");
        // httpGet("https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/textUpload")
        location.reload()
    }

    
}

//codepipeline

function send_data(){
    var file = document.getElementById('myfile').files[0];
    console.log("file ====>>>", file)
    if (file == null){
        console.log("herre in no file");
        send_text();
    }
    else{
        upload_image();
    }
}

function onLoadEvent(){
    // return false;
    var clean_container = document.getElementById("main_feed");
    if (typeof(clean_container) != 'undefined' && clean_container != null)
    {
        clean_container.innerHTML = " ";
    }
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", "https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/dataFetch", false ); // false for synchronous request
    xmlHttp.setRequestHeader("Access-Control-Allow-Origin" ,"*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Methods", "*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
    xmlHttp.send("hello");
    res1 = xmlHttp.responseText;
    // console.log("RES1 --->>>", res1);
    res2 = JSON.parse(res1);
    // console.log("RES 2 in HTTP GET FUNCC---->", res2);
    res2 = res2['a'];
    // console.log("RES 2 Updated---->", res2);

    var upCounter = 1
    res2.forEach((element, index)=>{
    var upCountId = 'counter_'+String(upCounter)

    var user_post_id = element["user_id"]
    var name_post = element["post_title"];
    var text_to_display = element["content_text"];
    var post_tag = element["post_tag"];
    var up_count = element["upvotes"];
    var stamp = element["timestamp"];
    var image_path = "https://resieve-image.s3.amazonaws.com/"+String(element["content_image"]).trim();
    // console.log("IMAGE PATH S3=====>>", image_path)

    if (element["content_image"] == " Null" || element["content_image"] == "Null"){
        var newtext = `<div class="subforum1" id="main_feed">
            <div class="subforum-title">
                <h1>${name_post}</h1>
            </div>
            <div class="subforum-row">
                <div class="subforum-icon subforum-column center">
                    <i class="fa fa-user-circle center"></i>
                </div>
                <div class="subforum-description subforum-column">
                    <h4><a href="#">${post_tag}</a></h4>
                    <p>${text_to_display}</p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter(${upCountId})">Upvote</button>
                    <p id="${upCountId}" >Upvotes : "${up_count}"</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b> Posted by ${user_post_id} on </b>
                    <br>on <small>${stamp}</small>
                </div>
            </div>
        </div>`
    $(".container").append(newtext);
    }

    else{
    var newtext = `<div class="subforum1" id="main_feed">
            <div class="subforum-title">
                <h1>${name_post}</h1>
            </div>
            <div class="subforum-row">
                <div class="subforum-icon subforum-column center">
                    <i class="fa fa-user-circle center"></i>
                </div>
                <div class="subforum-description subforum-column">
                    <h4><a href="#">${post_tag}</a></h4>
                    <p><img src=${image_path} alt=${text_to_display} width=300 height=300></p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter("${upCountId}")">Upvote</button>
                    <p id="${upCountId}" >Upvotes : "${up_count}"</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b> Posted by ${user_post_id} on </b>
                    <br>on <small>${stamp}</small>
                </div>
            </div>
        </div>`

    $(".container").append(newtext);
    }
    // console.log(res2);

    upCounter = upCounter+1;
    });
}

