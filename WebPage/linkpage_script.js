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


// main function
async function getLinkResult() {
    communication = 0
    $(".table_container").html("");
    $(".table_container").hide();
    var db = getURLParameter("db");
    var tables = getURLParameter("linkto");
    var clicked_col = getURLParameter("clicked_col");
    var clicked_val = getURLParameter("clicked_val");

    for (table_index in tables) {
        table = tables[table_index]
        createTable(table);

        var s = database.ref('/' + db + '/link/' + table + '/' + clicked_col + '/' + clicked_val); // s is a list of dictionary
        var x = s.once("value").then(function(node) {
            jquery_createRow(db, table, node.val());
        })
        await x;
        document.getElementById("data_fetched").innerHTML = communication;
    }
    $(".table_container").show();
}


function createTable(table) {
    // create a table with table name
    var t = '<table id=' + table + ' border="1"><caption style="text-align:left">' + table.toString().toUpperCase() + '</caption></table>';
    $(".table_container").append(t);
}


function jquery_createRow(db, table, list) {
    if (list == null) {
        return;
    }
    console.log("list", list)
    if (list.length != 0) {
        if ($("#" + table.toString()).children().length == 1) {
            var header = '<tr>'
            for (var i in col_ref[table]) {
                // build column name (header) row
                header += '<th>' + col_ref[table][i] + '</th>';
            }
            header += '</tr>'
            // append it to html page
            $('#' + table.toString()).append(header);

        }
        for (row_index = 0; row_index < list.length; row_index++){
            var tr = '<tr>'
            row_data = list[row_index]
            for (var col_index in col_ref[table]) {
                // build datarows for sorted search output
                // if that column has foreign key relation, build link for it
                col_name = col_ref[table][col_index]
                var cell_value = row_data[col_name]
                // handle NULL case
                if (cell_value == null){
                    cell_value = "---"
                }
                if (col_name in link_key[table]) {
                    var tables = link_key[table][col_name].join("+")
                    var newpage = 'linkpage.html?db=' + db + '&linkto=' + tables + '&clicked_col=' + col_name + '&clicked_val=' + cell_value
                    tr += '<td>' + '<a href="' + newpage + '">' + cell_value  + '</a></td>';
                } else {
                    tr += '<td>' + cell_value + '</td>';
                }
                
            }
            tr += '</tr>';
            communication += 1
            $('#' + table.toString()).append(tr);
        }
    }
}