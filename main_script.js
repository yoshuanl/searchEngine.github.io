var database;
var db;
// column name 
var col = {
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

// columns that allows user to link to other tables
var link_key = {
    "city": { 'CountryCode': ['country'] },
    "country": { 'CountryCode': ['city', 'countrylanguage'] },
    "countrylanguage": { 'CountryCode': ['country'] },

    "employees": { 'emp_no': ['titles', 'dept_emp'] },
    "titles": { 'emp_no': ['employees'] },
    "departments": { 'dept_no': ['dept_emp'] },
    "dept_emp": { 'emp_no': ['employees'], 'dept_no': ['departments'] },

    "film": { 'film_id': ['film_actor'] },
    "actor": { 'actor_id': ['film_actor'] },
    "film_actor": { 'film_id': ['film'], 'actor_id': ['actor'] },
}


function init() {
    firebase.initializeApp({
        databaseURL: "https://sql-searchdb.firebaseio.com/",
        projectId: "sql-searchdb"
    });
    database = firebase.database();

}

$(init);


async function input() {
    count = {};
    sort = {};
    count_column = {}
    temp = {}

    /* Project requirement: Payload overhead*/
    payload = { "index_read": 0, "rows_retrieved": 0, "index_size": 0, "data_size": 0 };

    var x = sortKeywords()
    await x
    $("#index_read").text(payload["index_read"]);
    $("#index_size").text("(" + formatByteSize(payload["index_size"]) + ")");

    var x = generateTable(lengthofsearch = 5, tables = null)
    await x
    $(".table_container").show();
    $("#data_fetched").text(payload["rows_retrieved"]);
    $("#data_size").text("(" + formatByteSize(payload["data_size"]) + ")");
    $("p").show();
};


async function Expand(table) {
    // console.log("expand", table)
    var x = generateTable(lengthofsearch = null, tables = table)
    await x
    $('#' + table.toString() + ' tr:gt(5)').show();
    $("#data_fetched").text(payload["rows_retrieved"]);
    $("#data_size").text("(" + formatByteSize(payload["data_size"]) + ")");
}


function hideAll(table) {
    $('#' + table.toString() + ' tr:gt(5)').hide()
    // scroll to top after hiding current table
    var x = $('#' + table.toString()).position();
    window.scroll({
        top: x.top - 110, 
        left: x.left, 
        behavior: 'auto'
    });
}


async function sortKeywords() {
    //console.log("2")
    $(".table_container").html("");
    $(".table_container").hide();

    var db = $('#db option:selected').text()
    if (db == "Select DB:") {
        var message1 = '<div>Please Select a Database.</div>';
        $(".table_container").append(message1);
        return
    }

    // normalized keywords
    var keywords = $("#kw").val().toLowerCase()
    keywords = keywords.replace(/(~|`|!|@|#|$|%|^|&|\*|\(|\)|{|}|\[|\]|;|:|\"|'|<|,|\.|>|\?|\/|\\|\||-|_|\+|=)/g, '')
        .split(/(\s+)/).filter(e => e.trim().length > 0);
    console.log(keywords);

    // use inverted index to count occurance of keywords in each table
    for (i = 0; i < keywords.length; i++) {
        // console.log(keywords[i])
        var route = db + '/index/' + keywords[i]
        var s = database.ref(route);

        var xx =
            s.once("value").then(function(node) {
                if (node.val() == null) {
                    return
                }
                node.forEach(function(child) {
                    var table = child.child("TABLE").val()
                    var key = child.child("PK").val()
                    if (!(table in count)) {
                        count[table] = {};
                        count_column[table] = {};
                        temp[table] = {}
                    };

                    if (!(key in count[table])) {
                        count[table][key.toString()] = 1;
                        count_column[table][key.toString()] = {}
                        count_column[table][key.toString()]["WC"] = 0
                        count_column[table][key.toString()]["CC"] = 0
                        count_column[table][key.toString()]["TC"] = 1
                        count_column[table][key.toString()]["TT"] = {}

                        temp[table][key.toString()] = {}
                        temp[table][key.toString()]["W"] = new Set()
                    } else {
                        count[table][key.toString()] += 1;
                        count_column[table][key.toString()]["TC"] += 1;
                    }
                    payload["index_read"] += 1;

                    var column_name = child.child("COLUMN").val()
                    if (!(column_name in count_column[table][key.toString()]["TT"])) {
                        count_column[table][key.toString()]["CC"] += 1
                        count_column[table][key.toString()]["CC"][column_name] = 0
                        count_column[table][key.toString()]["TT"][column_name] = 0
                        temp[table][key.toString()][column_name] = new Set()
                    }
                    if (!(keywords[i] in temp[table][key.toString()][column_name])) {
                        count_column[table][key.toString()]["TT"][column_name] += 1
                        temp[table][key.toString()][column_name].add(keywords[i])
                    }

                    if (!temp[table][key.toString()]["W"].has(keywords[i])) {
                        count_column[table][key.toString()]["WC"] += 1
                        temp[table][key.toString()]["W"].add(keywords[i])

                    }
                })
                payload["index_size"] += memorySizeOf(node.val());
                payload["index_size"] += memorySizeOf(route);
            });
        await xx;
    }

    if (Object.keys(count).length == 0) {
        var message1 = '<div>Sorry, no "' + keywords + '" exists in ' + db + ' database.</div>';
        $(".table_container").append(message1);
        return
    }

    // sort count result
    for (var i in count) {
        // console.log("i in count", i)

        tt = Object.keys(count_column[i]).sort(function(a, b) {
            if (count_column[i][a]["WC"] == keywords.length || count_column[i][a]["WC"] == keywords.length) {
                ma = Math.max(...Object.values(count_column[i][a]['TT']))
                mb = Math.max(...Object.values(count_column[i][b]['TT']))
                return -count_column[i][a]["WC"] + count_column[i][b]["WC"] || -ma + mb || -count_column[i][a]["TC"] + count_column[i][b]["TC"];
            }
            return -count_column[i][a]["WC"] + count_column[i][b]["WC"] || -count_column[i][a]["TC"] + count_column[i][b]["TC"];
        });
        // for (var j in tt) {
        //     console.log(count_column[i][tt[j]], Math.max(...Object.values(count_column[i][tt[j]]['TT'])));
        // }
        keysSorted = Object.keys(count[i]).sort(function(a, b) { return -count[i][a] + count[i][b] });
        sort[i] = tt;
        createTable(i, keysSorted.length);
    }

}


async function generateTable(lengthofsearch = null, tables = null) {
    // console.log("table", tables)
    // console.log("3")
    // console.log("sort", sort)
    // console.log("count", count)
    db = $('#db option:selected').text()
    for (var table in count) {
        start = Math.max($('#' + table.toString() + ' tr').length - 1, 0)
        if (tables != null & table != tables) {
            continue
        } else if (tables != null) {
            $('#' + tables.toString() + ' tr:gt(' + start + ')').hide()
        }

        if (lengthofsearch == null) {
            //lengthofsearch = sort[table].length + 1
            end = sort[table].length + 1
        } else {
            end = lengthofsearch
        }

        for (var id in sort[table].slice(start, end)) {
            var primary_key = sort[table][start + parseInt(id)];
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


function createTable(table, lengthoftable) {
    // create a table with table name and expand/collapse buttons
    var t = '<table id=' + table + ' border="1" align="center"><caption style="text-align:left">' + table.toString().toUpperCase();

    if (lengthoftable <= 5) {
        t += '</caption></table>'
        $('.table_container').append(t);
        return
    }
    t += '  <span class="right" style="text-align:right"><button class="collapse" onclick = "hideAll(\'' + table + '\')"> Collapse </button></span></caption></table>'
    collapse = $(t).click(function() { hideAll(table) })
    $('.table_container').append(collapse);

    var expand = '<span class="center"><button class="expand" > Expand </button></span>' //<hr />'
    var collapse = '<span class="center"><button class="collapse" > Collapse </button></span>'

    expand = $(expand).click(function() { Expand(table) })
    collapse = $(collapse).click(function() { hideAll(table) })

    $(".table_container").append(expand);
    $(".table_container").append(collapse);
}


function jquery_createRow(db, table, list) {
    if (list == null) {
        // console.log("list is null")
        return;
    }
    if (list.length != 0) {
        if ($("#" + table.toString()).children().length == 1) {
            var header = '<tr>'
            for (var i in col[table]) {
                // build column name (header) row
                header += '<th>' + col[table][i] + '</th>';
            }
            header += '</tr>'
                // append it to html page
            $('#' + table.toString()).append(header);
        }
        // build data rows for sorted search output
        var tr = '<tr>'
            // var rowCount = $('#' + table.toString() + ' tr').length;
            // console.log("4", rowCount);
        for (var col_index in col[table]) {

            var cell_value = list[col[table][col_index]]
                // handle NULL case
            if (cell_value == null | cell_value == "" | cell_value == "–") {
                cell_value = "---"
            }
            col_name = col[table][col_index]
                // if that column has foreign key relation, build link for it
            if (col_name in link_key[table]) {
                var tables = link_key[table][col_name].join("+")
                var newpage = 'linkpage.html?db=' + db + '&linkto=' + tables + '&clicked_col=' + col_name + '&clicked_val=' + cell_value
                tr += '<td>' + '<a href="' + newpage + '" target="_blank">' + cell_value + '</a></td>';
            } else {
                tr += '<td>' + cell_value + '</td>';
            }
        }
        tr += '</tr>';
        $('#' + table.toString()).append(tr);

        payload["rows_retrieved"] += 1
    }

}


/* Other additional functions */

// Allow user to press enter to fire search
$(document).ready(function() {
    $('#kw').keypress(function(e) {
        if (e.keyCode == 13)
            $('#searchbtn').click();
    });
    $("html,body").animate({ scrollTop: 0 }, 100);
});


// CSS
// When the user scrolls down 25px from the top of the document, resize the panel's padding and the title's font size
window.onscroll = function() { scrollFunction() };

function scrollFunction() {
    if ($(window).scrollTop > 25 || document.documentElement.scrollTop > 25) {
        // document.getElementById("panel").style.padding = "5px 5px 10px"; /* Top:5px, Right, Left: 5px, Bottom: 10px */
        $('#panel').css("padding", "5px 5px 10px");
        // document.getElementById("our_title").style.fontSize = "22px";
        $("#our_title").css("fontSize", "22px");
    } else {
        // document.getElementById("panel").style.padding = "40px 5px";
        $('#panel').css("padding", "40px 5px");
        // document.getElementById("our_title").style.fontSize = "35px";
        $("#our_title").css("fontSize", "35px");
    }
}


// Calculate size of an object, used in calculating communication overhead
// written according to firebase document: https://firebase.google.com/docs/firestore/storage-size#document-size
function memorySizeOf(obj) {
    var bytes = 0;

    function sizeOf(obj) {
        //console.log("obj:", obj)
        //console.log("type:", typeof obj)
        if (obj !== null && obj !== undefined) {
            switch (typeof obj) {
                case 'number':
                    bytes += 8;
                    break;
                case 'string':
                    if (obj.search("/") >= 0) {
                        bytes += 16; // 16 additional bytes for the path to the document
                        var subobjs = obj.split("/"); // collection ID, document ID along the path
                        //console.log("subobjs", subobjs)
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
                    if (objClass === 'Object') {
                        // 32 additional bytes for document
                        bytes += 32;
                        // field names
                        sizeOf(Object.keys(obj));
                        // values
                        for (var key in obj) {
                            if (!obj.hasOwnProperty(key)) continue;
                            sizeOf(obj[key]);
                        }
                    } else if (objClass === 'Array') {
                        //console.log("array!")
                        for (value in obj) {
                            sizeOf(obj[value]);
                        }
                    }
                    break;
            }
        } else {
            //console.log("null!")
            bytes += 1; // 1 byte for NULL
        }
        //console.log("object: ", obj, " cum size: ", bytes)
        return bytes;
    };
    return sizeOf(obj);
};


//var test1 = 'users/jeff/tasks/my_task_id'
//var test2 = {"type": "Personal", "done": {"E":90, "M":100}, "priority": 1, "description": "Learn Cloud Firestore"}
//console.log("size:", memorySizeOf(test1))
//console.log("size:", memorySizeOf(test2))


function formatByteSize(bytes) {
    if (bytes < 1024) return bytes + " bytes";
    else if (bytes < 1048576) return (bytes / 1024).toFixed(2) + " KB";
    else if (bytes < 1073741824) return (bytes / 1048576).toFixed(2) + " MB";
    else return (bytes / 1073741824).toFixed(2) + " GB";
};