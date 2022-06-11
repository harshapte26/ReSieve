// //NavBar
// function hideIconBar(){
//     var iconBar = document.getElementById("iconBar");
//     var navigation = document.getElementById("navigation");
//     iconBar.setAttribute("style", "display:none;");
//     navigation.classList.remove("hide");
// }

// function showIconBar(){
//     var iconBar = document.getElementById("iconBar");
//     var navigation = document.getElementById("navigation");
//     iconBar.setAttribute("style", "display:block;");
//     navigation.classList.add("hide");
// }

// //Comment
// function showComment(){
//     var commentArea = document.getElementById("comment-area");
//     commentArea.classList.remove("hide");
// }

// //Reply
// function showReply(){
//     var replyArea = document.getElementById("reply-area");
//     replyArea.classList.remove("hide");
// }
// var url = "http://127.0.0.1:5000/"


var name_post;
var text_to_display;
var post_tag;
var up_count;

function httpGet(theUrl)
{
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", theUrl, false ); // false for synchronous request
    // xmlHttp.setRequestHeader("Access-Control-Allow-Origin" ,"*");
    // xmlHttp.setRequestHeader("Access-Control-Allow-Methods", "*");
    // xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
    xmlHttp.send( null );
    res1 = xmlHttp.responseText;
    res2 = JSON.parse(res1);
    console.log("res2---->", res2)

    res2.forEach((element, index)=>{
    name_post = res2["post_title"];
    text_to_display = res2["content"];
    post_tag = res2["tag"];
    up_count = res2["up_count"];

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
                    <p><img src="https://images.hindustantimes.com/img/2021/09/18/1600x900/309456.7_1631970506804_1631970903630.jpg" style="width:128px;height:128px;"></p>
                </div>
                <div class="subforum-stats subforum-column center">
                    <span>
                    <button type="button" onclick="update_counter()">Upvote</button>
                    <p id="counter" >Upvote Count : ${up_count}</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b><a href="">Last post</a></b> by <a href="">JustAUser</a> 
                    <br>on <small>12 Dec 2020</small>
                </div>
            </div>
        </div>`

    $(".container").append(newtext);
    console.log(res2);
    });

    // name_post = res2["post_title"];
    // text_to_display = res2["content"];
    // post_tag = res2["tag"];
    // up_count = res2["up_count"];

    // var newtext = `<div class="subforum1">
    //         <div class="subforum-title">
    //             <h1>${name_post}</h1>
    //         </div>
    //         <div class="subforum-row">
    //             <div class="subforum-icon subforum-column center">
    //                 <i class="fa fa-car center"></i>
    //             </div>
    //             <div class="subforum-description subforum-column">
    //                 <h4><a href="#">${post_tag}</a></h4>
    //                 <p><img src="https://images.hindustantimes.com/img/2021/09/18/1600x900/309456.7_1631970506804_1631970903630.jpg" style="width:128px;height:128px;"></p>
    //             </div>
    //             <div class="subforum-stats subforum-column center">
    //                 <span>
    //                 <button type="button" onclick="update_counter()">Upvote</button>
    //                 <p id="counter" >Upvote Count : ${up_count}</p>
    //                 </span>
    //             </div>
    //             <div class="subforum-info subforum-column">
    //                 <b><a href="">Last post</a></b> by <a href="">JustAUser</a> 
    //                 <br>on <small>12 Dec 2020</small>
    //             </div>
    //         </div>
    //     </div>`

    // $(".container").append(newtext);
    // console.log(res2);
}

function update_counter(){
    document.getElementById("counter").innerHTML = "Upvote Count :" + String(++up_count);
}

// function upload_image(){
//     // var file = document.getElementById('file_path').files[0];
//     var file_name = document.getElementById("myfile").mozFullPath;
//     console.log("file_name =====>> ", file_name)
// }

function upload_image(){
var tag_image = document.getElementById('post_tag').value;
var file = document.getElementById('myfile').files[0];
var type_file_up = file.type;
var name_f = file.name;
console.log("File Type ===>", type_file_up)


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
    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "POST", "https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/dataUpload", false ); // false for synchronous request
    xmlHttp.setRequestHeader("x-amz-meta-customLabels" ,tag_image);
    xmlHttp.setRequestHeader("Content-Type", type_file_up);
    xmlHttp.setRequestHeader("filename", name_f)
    // xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");

    xmlHttp.send(base64String);
    res1 = xmlHttp.responseText;
    res2 = JSON.parse(res1);
    console.log("RESPONSE ----->>>>>>>", res2)
    
    if(res2['isValid'] === 1){
        console.log("ITS APRROVED!!!!!")
        alert("Image Uploaded Successfully")
    }
    else{
        alert("Image is not appropriate according to guidelines, please upload different image")
    }
}
reader.readAsDataURL(file);

// console.log("base 64 image --->>>", base64String)

// var reader = new FileReader();
// reader.readAsArrayBuffer(file);
// reader.onload = function (event) {
//     console.log("Reader Object",event.target.result)
//     console.log("Reader ",reader.result)
//     // var myHeaders = new Headers();
//     // myHeaders.append("x-amz-meta-customLabels",  note_customtag.value);
//     // myHeaders.append("Content-Type", type_file_up);
//     // console.log("Header here --->>>", myHeaders)
//     var file = new Uint8Array(reader.result);
//     // var requestOptions = {
//     // method: 'PUT',
//     // headers: {"x-amz-meta-customLabels" : tag_image, "Content-Type": type_file_up},
//     // body: file,
//     // redirect: 'follow'
//     // };

//     var xmlHttp = new XMLHttpRequest();
//     xmlHttp.open( "POST", "https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/dataUpload", false ); // false for synchronous request
//     xmlHttp.setRequestHeader("x-amz-meta-customLabels" ,tag_image);
//     xmlHttp.setRequestHeader("Content-Type", type_file_up);
//     // xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
//     xmlHttp.send(file);
//     res1 = xmlHttp.responseText;
//     res2 = JSON.parse(res1);
//     console.log("RESPONSE ----->>>>>>>", res2)


//     // console.log("re headers ====>>>>", requestOptions);
//     // fetch(`https://16577esc56.execute-api.us-east-1.amazonaws.com/alpha/upload/album-photo-store/${name_f}`, requestOptions)
//     // .then(response => response.text())
//     // .then(result => console.log(result)).then(alert("Photo uploaded successfully!"))
//     // .catch(error => console.log('error', error));
// }
}


function mytest(){
    var name = document.getElementById("post_title").value;
    var text_to_display = document.getElementById("newthread").value;
    var post_tag = document.getElementById("post_tag").value;
    var up_count = 24

    var xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "POST", "http://127.0.0.1:5000/search", false ); // false for synchronous request
    xmlHttp.setRequestHeader("Access-Control-Allow-Origin" ,"*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Methods", "*");
    xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
    xmlHttp.send( null );
    res1 = xmlHttp.responseText;
    res2 = JSON.parse(res1);

    if (res2['isValidContent'] != 1){
        alert("this is offensive content you can't post this")
    }
    else{

    var newtext = `<div class="subforum1">
            <div class="subforum-title">
                <h1>${name}</h1>
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
                    <button type="button">Upvote</button>
                    <p>Upvote Count : ${up_count}</p>
                    </span>
                </div>
                <div class="subforum-info subforum-column">
                    <b><a href="">Last post</a></b> by <a href="">JustAUser</a> 
                    <br>on <small>12 Dec 2020</small>
                </div>
            </div>
        </div>`

    $(".container").append(newtext);      // Append the new elements
    }

    // // Creating a div element
    //     var divElement = document.createElement("Div");
    //     divElement.id = "divID";

    //     // Styling it
    //     divElement.style.textAlign = "center";
    //     divElement.style.fontWeight = "bold";
    //     divElement.style.fontSize = "smaller";
    //     divElement.style.paddingTop = "15px";

    //     // Adding a paragraph to it
    //     var paragraph = document.createElement("P");
    //     var text = document.createTextNode(text_to_display);
    //     paragraph.appendChild(text);
    //     divElement.appendChild(paragraph);

    //     // Adding a button, cause why not!
    //     var button = document.createElement("Button");
    //     var textForButton = document.createTextNode("Release the alert");
    //     button.appendChild(textForButton);
    //     button.addEventListener("click", function(){
    //         alert("Hi!");
    //     });
    //     divElement.appendChild(button);

    //     // Appending the div element to body
    //     document.getElementsByTagName("body")[0].appendChild(divElement);
}



var div_ele = document.createElement()


//------------------<<<<<>>>>>-----------------

// //NavBar
// function hideIconBar(){
//     var iconBar = document.getElementById("iconBar");
//     var navigation = document.getElementById("navigation");
//     iconBar.setAttribute("style", "display:none;");
//     navigation.classList.remove("hide");
// }

// function showIconBar(){
//     var iconBar = document.getElementById("iconBar");
//     var navigation = document.getElementById("navigation");
//     iconBar.setAttribute("style", "display:block;");
//     navigation.classList.add("hide");
// }

// //Comment
// function showComment(){
//     var commentArea = document.getElementById("comment-area");
//     commentArea.classList.remove("hide");
// }

// //Reply
// function showReply(){
//     var replyArea = document.getElementById("reply-area");
//     replyArea.classList.remove("hide");
// }
// var url = "http://127.0.0.1:5000/"


// var name_post;
// var text_to_display;
// var post_tag;


// <b><a href="">Posted By </a></b> by <a href="">JustAUser</a>

    // name_post = res2["post_title"];
    // text_to_display = res2["content"];
    // post_tag = res2["tag"];
    // up_count = res2["up_count"];

    // var newtext = `<div class="subforum1">
    //         <div class="subforum-title">
    //             <h1>${name_post}</h1>
    //         </div>
    //         <div class="subforum-row">
    //             <div class="subforum-icon subforum-column center">
    //                 <i class="fa fa-car center"></i>
    //             </div>
    //             <div class="subforum-description subforum-column">
    //                 <h4><a href="#">${post_tag}</a></h4>
    //                 <p><img src="https://images.hindustantimes.com/img/2021/09/18/1600x900/309456.7_1631970506804_1631970903630.jpg" style="width:128px;height:128px;"></p>
    //             </div>
    //             <div class="subforum-stats subforum-column center">
    //                 <span>
    //                 <button type="button" onclick="update_counter()">Upvote</button>
    //                 <p id="counter" >Upvote Count : ${up_count}</p>
    //                 </span>
    //             </div>
    //             <div class="subforum-info subforum-column">
    //                 <b><a href="">Last post</a></b> by <a href="">JustAUser</a> 
    //                 <br>on <small>12 Dec 2020</small>
    //             </div>
    //         </div>
    //     </div>`

    // $(".container").append(newtext);
    // console.log(res2);

    // <b><a href="">Posted By </a></b> by <a href="">JustAUser</a>

    // name_post = res2["post_title"];
    // text_to_display = res2["content"];
    // post_tag = res2["tag"];
    // up_count = res2["up_count"];

    // var newtext = `<div class="subforum1">
    //         <div class="subforum-title">
    //             <h1>${name_post}</h1>
    //         </div>
    //         <div class="subforum-row">
    //             <div class="subforum-icon subforum-column center">
    //                 <i class="fa fa-car center"></i>
    //             </div>
    //             <div class="subforum-description subforum-column">
    //                 <h4><a href="#">${post_tag}</a></h4>
    //                 <p><img src="https://images.hindustantimes.com/img/2021/09/18/1600x900/309456.7_1631970506804_1631970903630.jpg" style="width:128px;height:128px;"></p>
    //             </div>
    //             <div class="subforum-stats subforum-column center">
    //                 <span>
    //                 <button type="button" onclick="update_counter()">Upvote</button>
    //                 <p id="counter" >Upvote Count : ${up_count}</p>
    //                 </span>
    //             </div>
    //             <div class="subforum-info subforum-column">
    //                 <b><a href="">Last post</a></b> by <a href="">JustAUser</a> 
    //                 <br>on <small>12 Dec 2020</small>
    //             </div>
    //         </div>
    //     </div>`

    // $(".container").append(newtext);
    // console.log(res2);
    
    // console.log("base 64 image --->>>", base64String)

// var reader = new FileReader();
// reader.readAsArrayBuffer(file);
// reader.onload = function (event) {
//     console.log("Reader Object",event.target.result)
//     console.log("Reader ",reader.result)
//     // var myHeaders = new Headers();
//     // myHeaders.append("x-amz-meta-customLabels",  note_customtag.value);
//     // myHeaders.append("Content-Type", type_file_up);
//     // console.log("Header here --->>>", myHeaders)
//     var file = new Uint8Array(reader.result);
//     // var requestOptions = {
//     // method: 'PUT',
//     // headers: {"x-amz-meta-customLabels" : tag_image, "Content-Type": type_file_up},
//     // body: file,
//     // redirect: 'follow'
//     // };

//     var xmlHttp = new XMLHttpRequest();
//     xmlHttp.open( "POST", "https://548zfv0fek.execute-api.us-east-1.amazonaws.com/alpha/dataUpload", false ); // false for synchronous request
//     xmlHttp.setRequestHeader("x-amz-meta-customLabels" ,tag_image);
//     xmlHttp.setRequestHeader("Content-Type", type_file_up);
//     // xmlHttp.setRequestHeader("Access-Control-Allow-Headers", "*");
//     xmlHttp.send(file);
//     res1 = xmlHttp.responseText;
//     res2 = JSON.parse(res1);
//     console.log("RESPONSE ----->>>>>>>", res2)


//     // console.log("re headers ====>>>>", requestOptions);
//     // fetch(`https://16577esc56.execute-api.us-east-1.amazonaws.com/alpha/upload/album-photo-store/${name_f}`, requestOptions)
//     // .then(response => response.text())
//     // .then(result => console.log(result)).then(alert("Photo uploaded successfully!"))
//     // .catch(error => console.log('error', error));
// }


// var newtext = `<div class="subforum1">
    //         <div class="subforum-title">
    //             <h1>${name}</h1>
    //         </div>
    //         <div class="subforum-row">
    //             <div class="subforum-icon subforum-column center">
    //                 <i class="fa fa-car center"></i>
    //             </div>
    //             <div class="subforum-description subforum-column">
    //                 <h4><a href="#">${post_tag}</a></h4>
    //                 <p>${text_to_display}</p>
    //             </div>
    //             <div class="subforum-stats subforum-column center">
    //                 <span>
    //                 <button type="button">Upvote</button>
    //                 <p>Upvote Count : ${up_count}</p>
    //                 </span>
    //             </div>
    //             <div class="subforum-info subforum-column">
    //                 <b><a href="">Last post</a></b> by <a href="">JustAUser</a> 
    //                 <br>on <small>12 Dec 2020</small>
    //             </div>
    //         </div>
    //     </div>`

    // $(".container").append(newtext);      // Append the new elements

    // // Creating a div element
    //     var divElement = document.createElement("Div");
    //     divElement.id = "divID";

    //     // Styling it
    //     divElement.style.textAlign = "center";
    //     divElement.style.fontWeight = "bold";
    //     divElement.style.fontSize = "smaller";
    //     divElement.style.paddingTop = "15px";

    //     // Adding a paragraph to it
    //     var paragraph = document.createElement("P");
    //     var text = document.createTextNode(text_to_display);
    //     paragraph.appendChild(text);
    //     divElement.appendChild(paragraph);

    //     // Adding a button, cause why not!
    //     var button = document.createElement("Button");
    //     var textForButton = document.createTextNode("Release the alert");
    //     button.appendChild(textForButton);
    //     button.addEventListener("click", function(){
    //         alert("Hi!");
    //     });
    //     divElement.appendChild(button);

    //     // Appending the div element to body
    //     document.getElementsByTagName("body")[0].appendChild(divElement);