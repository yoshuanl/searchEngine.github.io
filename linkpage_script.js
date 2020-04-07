// columns reference
var col_ref = {
    "city": ['ID', 'Name', 'CountryCode', 'District', 'Population'],
    "country": ['CountryCode', 'Name', 'Continent', 'Region', 'SurfaceArea', 'IndepYear', 'Population', 'LifeExpectancy', 'GNP', 'GNPOld', 'LocalName', 'GovernmentForm', 'HeadOfState', 'Capital', 'Code2'],
    "countrylanguage": ['CountryCode', 'Language', 'IsOfficial', 'Percentage'],
    
    "employees": ['emp_no', 'birth_date', 'first_name', 'last_name', 'gender', 'hire_date'],
    "titles": ['emp_no', 'title', 'from_date', 'to_date'],
    "departments": ['dept_no', 'dept_name'],
    "dept_emp": ['emp_no', 'dept_no', 'from_date', 'to_date'],

    "film": ['film_id', 'title', 'description', 'length'],
    "actor": ['actor_id', 'first_name', 'last_name'],
    "film_actor": ['film_id', 'actor_id']
}

// foreign key relationship
// columns that allows user to link to other tables
var link_key = {
    "city": {'CountryCode': ['country']},
    "country": {'CountryCode': ['city', 'countrylanguage']},
    "countrylanguage": {'CountryCode': ['country']},
    
    "employees": {"emp_no": ["titles", "dept_emp"]},
    "titles": {"emp_no": ["employees"]},
    "departments": {"dept_no": ["dept_emp"]},
    "dept_emp": {"emp_no": ["employees"], "dept_no": ["departments"]},

    "film": { 'film_id': ['film_actor'] },
    "actor": { 'actor_id': ['film_actor'] },
    "film_actor": { 'film_id': ['film'], 'actor_id': ['actor'] },
}


// initialize firebase
// load page
function init() {
    firebase.initializeApp({
        databaseURL: "https://sql-searchdb.firebaseio.com/",
        projectId: "sql-searchdb"
    });
    database = firebase.database();
    getLinkResult();
}
$(init);


// get parameter from url
function getURLParameter(sParam) {
    var sPageURL = window.location.search.substring(1);
    var sURLVariables = sPageURL.split('&');
    for (var i = 0; i < sURLVariables.length; i++) { 
        var sParameterName = sURLVariables[i].split('=');
        if (sParameterName[0] == sParam) {
            if (sParam == "linkto"){
                return sParameterName[1].split('+');
            }
            return sParameterName[1];
        }
    }
}


async function Expand(table) {
    // console.log("expand", table)
    var x = generateTable(lengthofsearch = null, target_table = table)
    await x
    $('#' + table.toString() + ' tr:gt(5)').show()
    $("#data_fetched").text(payload["rows_retrieved"]);
    $("#data_size").text("(" + formatByteSize(payload["data_size"]) + ")");
}


function hideAll(table) {
    $('#' + table.toString() + ' tr:gt(5)').hide()
}


// main function
async function getLinkResult() {
    result = {}

    /* Project requirement: Payload overhead*/
    payload = { "index_read": 0, "rows_retrieved": 0, "index_size": 0, "data_size":0 }; 
    
    $(".linkpage_table_container").html("");
    $(".linkpage_table_container").hide();

    db = getURLParameter("db");

    var linked_tables = getURLParameter("linkto");
    var clicked_col = getURLParameter("clicked_col");
    var clicked_val = getURLParameter("clicked_val");

    for (table_index in linked_tables) {
        table = linked_tables[table_index]
        var route = db + '/link/' + table + '/' + clicked_col + '/' + clicked_val;
        var s = database.ref(route);
        var y = s.once("value").then(function(node) {
            // handle no data found in foreign key relationship table
            if (node.val() == null){
                return
            }
            result[table] = node.val();
            createTable(table);
            payload["index_size"] += memorySizeOf(node.val());
            payload["index_size"] += memorySizeOf(route);
            payload["index_read"] += result[table].length;
        })
        await y;
    }
    
    if (Object.keys(result).length != 0) {
        var x = generateTable(lengthofsearch = 5, target_table = null);
        await x;
    } else {
        var message1 = '<div>No Data Found in Other Tables for ' + clicked_val + '</div>';
        $(".linkpage_table_container").append(message1);
    }

    $("#index_read").text(payload["index_read"]);
    $("#index_size").text("(" + formatByteSize(payload["index_size"]) + ")");
    $("#data_fetched").text(payload["rows_retrieved"]);
    $("#data_size").text("(" + formatByteSize(payload["data_size"]) + ")");

    $(".linkpage_table_container").show();
}


// build column name (header) row
function createHeader(table) {
    var header = '<tr>'
    for (var i in col_ref[table]) {
        header += '<th>' + col_ref[table][i] + '</th>';
    }
    header += '</tr>'
    $('#' + table.toString()).append(header);
}


function createTable(table) {
    // create a table with table name
    var t = '<table id=' + table + ' border="1"><caption style="text-align:left">' + table.toString().toUpperCase();
    if (result[table].length <= 5) {
        t += '</caption></table>'
        $('.linkpage_table_container').append(t);
        createHeader(table)
        return
    }

    t += '  <span class="right" style="text-align:right"><button class="collapse"> Collapse </button></span></caption></table>'
    collapse = $(t).click(function() { hideAll(table) })
    $('.linkpage_table_container').append(collapse);

    var expand = '<span class="center"><button class="expand" > Expand </button></span>' //<hr />'
    var collapse = '<span class="center"><button class="collapse" > Collapse </button></span>'

    expand = $(expand).click(function() { Expand(table) })
    collapse = $(collapse).click(function() { hideAll(table) })

    $(".linkpage_table_container").append(expand);
    $(".linkpage_table_container").append(collapse);
    createHeader(table)
}


async function generateTable(lengthofsearch = null, target_table = null) {
    for (var table in result) {
        start = Math.max($('#' + table.toString() + ' tr').length - 1, 0)
        if (target_table != null & table != target_table) {
            continue
        } else if (target_table != null) {
            $('#' + target_table.toString() + ' tr:gt(' + start + ')').hide()
        }
        
        if (lengthofsearch != null) {
            payload["rows_retrieved"] += Math.min(lengthofsearch, result[table].length);
            end = lengthofsearch
        } else {
            payload["rows_retrieved"] += result[table].length - start;
            end = result[table].length + 1;
        }

        for (var id in result[table].slice(start, end)) {
            var primary_key = result[table][start + parseInt(id)];
            // retreive data from firebase
            var route = db + "/" + table + '/' + primary_key;
            var s = database.ref(route);
            var x = s.once("value").then(function(node) {
                jquery_createRow(db, table, node.val());
                payload["data_size"] += memorySizeOf(node.val());
            })
            payload["data_size"] += memorySizeOf(route);
        }
        await x;
    };
}


// this function build one data row
function jquery_createRow(db, table, list) {
    if (list == null) {
        return;
    }
    var tr = '<tr>'
    for (var col_index in col_ref[table]) {
        var cell_value = list[col_ref[table][col_index]]
            // handle NULL case
        if (cell_value == null | cell_value == "" | cell_value == "–") {
            cell_value = "---"
        }
        col_name = col_ref[table][col_index]
        if (col_name in link_key[table]) {
            var tables = link_key[table][col_name].join("+")
            var newpage = 'linkpage.html?db=' + db + '&linkto=' + tables + '&clicked_col=' + col_name + '&clicked_val=' + cell_value
            tr += '<td>' + '<a href="' + newpage + '">' + cell_value  + '</a></td>';
        } else {
            tr += '<td>' + cell_value + '</td>';
        }     
    }
    tr += '</tr>';
    $('#' + table.toString()).append(tr);
}


// Calculate size of an object, used in calculating communication overhead
// written according to firebase document: https://firebase.google.com/docs/firestore/storage-size#document-size
function memorySizeOf(obj) {
    var bytes = 0;

    function sizeOf(obj) {
        console.log("obj:", obj)
        console.log("type:", typeof obj)
        if(obj !== null && obj !== undefined) {
            switch(typeof obj) {
            case 'number':
                bytes += 8;
                break;
            case 'string':
                if (obj.search("/") >= 0) {
                    bytes += 16; // 16 additional bytes for the path to the document
                    var subobjs = obj.split("/"); // collection ID, document ID along the path
                    console.log("subobjs", subobjs)
                    for (name in subobjs) {
                        sizeOf(subobjs[name]);
                    }
                } else bytes += obj.length + 1
                break;
            case 'boolean':
                bytes += 1;
                break;
            case 'object':
                var objClass = Object.prototype.toString.call(obj).slice(8, -1);
                if(objClass === 'Object') {
                    // 32 additional bytes for document
                    bytes += 32;
                    // field names
                    sizeOf(Object.keys(obj));
                    // values
                    for(var key in obj) {
                        if(!obj.hasOwnProperty(key)) continue;
                        sizeOf(obj[key]);
                    }
                } else if (objClass === 'Array') {
                    console.log("array!")
                    for (value in obj) {
                        sizeOf(obj[value]);
                    }
                }
                break;
            }
        } else {
            console.log("null!")
            bytes += 1; // 1 byte for NULL
        }
        console.log("object: ", obj, " cum size: ", bytes)
        return bytes;
    };
    return sizeOf(obj);
};


//var test1 = 'users/jeff/tasks/my_task_id'
//var test2 = {"type": "Personal", "done": {"E":90, "M":100}, "priority": 1, "description": "Learn Cloud Firestore"}
//console.log("size:", memorySizeOf(test1))
//console.log("size:", memorySizeOf(test2))


function formatByteSize(bytes) {
    if(bytes < 1024) return bytes + " bytes";
    else if(bytes < 1048576) return(bytes / 1024).toFixed(2) + " KB";
    else if(bytes < 1073741824) return(bytes / 1048576).toFixed(2) + " MB";
    else return(bytes / 1073741824).toFixed(2) + " GB";
};