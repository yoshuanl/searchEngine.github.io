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
    communication = { "sort": 0, "retrieve": 0 }; /* Project requirement: Communication overhead*/

    var x = sortKeywords()
    await x
    $("#index_read").text(communication["sort"]);

    var x = generateTable(lengthofsearch = 5, tables = null)
    await x
    $(".table_container").show();
    $("#data_fetched").text(communication["retrieve"]);
};


async function Expand(table) {
    // console.log("expand", table)
    var x = generateTable(lengthofsearch = null, tables = table)
    await x
    $('#' + table.toString() + ' tr:gt(5)').show()
    $("#data_fetched").text(communication["retrieve"]);
}


function hideAll(table) {
    $('#' + table.toString() + ' tr:gt(5)').hide()
}


async function sortKeywords() {
    //console.log("2")
    $(".table_container").html("");
    $(".table_container").hide();

    var db = $('#db option:selected').text()
    console.log(db);
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
        var s = database.ref('/' + db + '/index/' + keywords[i]);
        var xx =
            s.once("value").then(function(node) {
                node.forEach(function(child) {

                    var table = child.child("TABLE").val()
                    var key = child.child("PK").val()
                    if (!(table in count)) {
                        count[table] = {};
                    };

                    if (!(key in count[table])) {
                        count[table][key.toString()] = 1;
                    } else {
                        count[table][key.toString()] += 1;
                    }
                    communication["sort"] += 1
                })
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
        keysSorted = Object.keys(count[i]).sort(function(a, b) { return -count[i][a] + count[i][b] });
        sort[i] = keysSorted;
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
            lengthofsearch = sort[table].length + 1
        }

        for (var id in sort[table].slice(start, lengthofsearch)) {
            var primary_key = sort[table][start + parseInt(id)];
            // retreive data from firebase
            var s = database.ref('/' + db + "/" + table + '/' + primary_key);
            var x = s.once("value").then(function(node) {
                jquery_createRow(db, table, node.val());
            })
        }
        await x;
        // document.getElementById("data_fetched").innerHTML = communication["retrieve"];
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
    t += '  <span class="right" style="text-align:right"><button class="collapse"> Collapse </button></span></caption></table>'
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
        communication["retrieve"] += 1
        $('#' + table.toString()).append(tr);

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