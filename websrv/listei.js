// http://underscorejs.org/     http://selectize.github.io/selectize.js/    https://github.com/selectize/selectize.js/blob/master/docs/usage.md     http://www.w3schools.com/js/default.asp     https://datatables.net/
// ----- Add del in tag : fonction appelee par button, l'ajout du del va entrainer le callback
function tags_add_del(doc_id, input_id_selectize)  {
    var tags_new = $('#'+input_id_selectize).val() ;
    if ( tags_new != "" ) { tags_new += ",---" ;}
    else { tags_new = "---" ; }
    var selectize_tags_ligne = $('#'+input_id_selectize)[0].selectize ;
    selectize_tags_ligne.addItem("---") ;
}
// ------ Callback de modif de TAG
function tags_modif(doc_id, compo_id, new_value) {
    $.ajax({
        "url": "./upd_obj_tags",
        "data": {
            "object_id" : doc_id,
            "str_tags_comma" : new_value
        }
    });
}

// ------ Get stats for init
function get_init() {
    // --- Get nb days histo from session
    var nb_days_histo2 = "7" ;
    if ( localStorage.getItem('nb_days_histo') != null )   { nb_days_histo2 = localStorage.getItem('nb_days_histo') ; }
    $('#input_nbdays')[0].value = nb_days_histo2

    $.getJSON(url="./get_init", data={"nb_days"    : nb_days_histo2 }, success=function( data1 )
    {
        $('#fd_user')[0].innerHTML          = data1['User'] ;
        $('#fd_datemin')[0].innerHTML       = data1['DateMin'] ;
        $('#fd_dateinterval')[0].innerHTML  = data1['DateInterval'] ;
        $('#fd_datemax')[0].innerHTML       = data1['DateMax'] ;
        $('#fd_nbtotal')[0].innerHTML       = data1['CountTotal'] ;
        $('#fd_nbselect')[0].innerHTML      = data1['CountSelected'] ;

        init_tag_fields() ;
    } ) ;
}

// ------ Init the tag fields
function init_tag_fields() {
     arr_tags_alls = arr_tags_show = arr_tags_hide = [] ;// Liste de tags tag1,tag2,tag3...
    // --- Recup depuis le server
    $.getJSON(url="./get_usrtags", data={ }, success=function( data2 )
    {
        // --- Listes des tags recus depuis le server
        arr_tags_alls = data2['alltags'] ; // liste globale qui donne tous les tags existants dans la DB
        arr_tags_show = data2['showtags'] ; // tags show selectionnes une fois precendante (user pref) a verifier
        arr_tags_hide = data2['hidetags'] ; // tags hide selectionnes une fois precendante (user pref) a verifier

        // --- Recup des tags show & hide depuis la session
        if ( localStorage.getItem('str_tags_show') != null )   { arr_tags_show = localStorage.getItem('str_tags_show').split(",") ; }
        if ( localStorage.getItem('str_tags_hide') != null )   { arr_tags_hide = localStorage.getItem('str_tags_hide').split(",") ; }

        // --- on enleve le "" des arrays de tags au besoin
        arr_tags_alls = _.compact(arr_tags_alls) ;
        arr_tags_show = _.compact(arr_tags_show) ;
        arr_tags_hide = _.compact(arr_tags_hide) ;

        // --- verif si tag dans show et hide, on enleve du hide
        if ( _.intersection(arr_tags_show,arr_tags_hide).length > 0 ) { arr_tags_hide = _.difference(arr_tags_hide, arr_tags_show) ; }

        // --- Verif tags all recus mais inconnus de hide et de show -> a ajouter dans show
        new_tags = _.difference(arr_tags_alls, _.union(arr_tags_show, arr_tags_hide)) ;
        arr_tags_show = _.union(arr_tags_show, new_tags) ;

        // --- Verif tags dans hide ou show mais pas dans la liste globale, on les enleve
        tags_a_eff    = _.difference(arr_tags_hide, arr_tags_alls) ; // Soustraction de liste : returns the values from array that are not present in the other arrays
        arr_tags_hide = _.difference(arr_tags_hide, tags_a_eff) ;
        tags_a_eff    = _.difference(arr_tags_show, arr_tags_alls) ; // Soustraction de liste : returns the values from array that are not present in the other arrays
        arr_tags_show = _.difference(arr_tags_show, tags_a_eff) ;

        // --- Implem des fields html
        liste_tags_all_selectize = [] ;
        fLen = arr_tags_alls.length ;
        for (i = 0; i < fLen; i++) {
            liste_tags_all_selectize.push({'value': arr_tags_alls[i], 'text': arr_tags_alls[i]}) ;
        }
        $('#input_tags_show').selectize({
            plugins: ['restore_on_backspace','remove_button', 'drag_drop'],
            delimiter: ',',
            options : liste_tags_all_selectize,             //options : [{value:'choix1', text:'choix1'}, {value:'choix2', text:'choix2'}],
            items : arr_tags_show,
            persist: true,
            create: false, // function(input) { return { value: input, text: input } },
            onItemRemove: function(value)           { tag_from_show_to_hide(value) ; }, // sera appele sur chaque item en cas de delete de selection
            onItemAdd:    function(value, $item)    { tag_from_hide_to_show(value) ; }
        });

        $('#input_tags_hide').selectize({
            plugins: ['restore_on_backspace','remove_button', 'drag_drop'],
            delimiter: ',',
            options : liste_tags_all_selectize,             //options : [{value:'choix1', text:'choix1'}, {value:'choix2', text:'choix2'}],
            items : arr_tags_hide,
            persist: true,
            create: false, // function(input) { return { value: input, text: input } }
            onItemRemove: function(value)           { tag_from_hide_to_show(value) ; },
            onItemAdd:    function(value, $item)    { tag_from_show_to_hide(value) ; }
        });

        // --- Creation de la datatable
        datable_creation() ;
    } ) ;
}

// ----- Tag from show to hide
function tag_from_show_to_hide(itemname) {
    var selectize_tags = $("#input_tags_hide")[0].selectize ; // normalement : tous les items dans dans la liste generale des options : selectize_tags.addOption({ text:itemname, value: itemname });
    selectize_tags.addItem(itemname) ;
    var selectize_tags = $("#input_tags_show")[0].selectize ;
    selectize_tags.removeItem(itemname) ;
    localStorage.setItem('str_tags_show', $('#input_tags_show').val()) ;
    localStorage.setItem('str_tags_hide', $('#input_tags_hide').val()) ;
}
// ----- Tag from hide to show
function tag_from_hide_to_show(itemname) {
    var selectize_tags = $("#input_tags_show")[0].selectize ;
    selectize_tags.addItem(itemname) ;
    var selectize_tags = $("#input_tags_hide")[0].selectize ; // normalement : tous les items dans dans la liste generale des options : selectize_tags.addOption({ text:itemname, value: itemname });
    selectize_tags.removeItem(itemname) ;
    localStorage.setItem('str_tags_show', $('#input_tags_show').val()) ;
    localStorage.setItem('str_tags_hide', $('#input_tags_hide').val()) ;
}

function butt_refresh() {
    // --- Tags Show & Hide dans la session
    str_tags_show = $('#input_tags_show').val() ;
    localStorage.setItem('str_tags_show', str_tags_show) ; // Le array devient un string,string,...
    str_tags_hide = $('#input_tags_hide').val() ;
    localStorage.setItem('str_tags_hide', str_tags_hide) ; // Le array devient un string,string,...
    // --- Nb jours dans la session
    nb_days_histo = $('#input_nbdays').val() ;
    localStorage.setItem('nb_days_histo', nb_days_histo) ; // Le array devient un string,string,...

    // --- Reload de la page
    location.reload(forceGet=true) ;
}

// --- Quand la page est prete
$(document).ready(function() {
    var gv_madatatable ;
    get_init() ;
} ) ;

// -- Load des data et creation de la table : Le parsing se fait mal dans editeur a cause du Regexp
function datable_creation() {
    // --- Ajout fonction FILTRE de plus (PUSH) qui sera testee pour afficher ou pas chaque ligne de la table : TAGS USER
    $.fn.dataTable.ext.search.push(function( settings, data, dataIndex ) {
        // data = le array de la ligne, donc data[0], data[1] ... pour les valeur de chaque champ
        var pos1 = data[4].indexOf(").selectize(",0) ;
        var pos2 = data[4].indexOf("items : [",pos1) + 9 ;
        var pos3 = data[4].indexOf("]",pos2) ; // "" ou "'test','test2'" avec les ' dedans
        var tags_ligne_str = data[4].substring(pos2,pos3).replace(/'/g,"") ;
        if ( tags_ligne_str == "" ) {
            return true ; // Par defaut on affiche les lignes sans tag
        } else {
            // Si "---" est bien dans le HIDE (par defaut c'est le cas) et si "---" dans les tags de l'objet alors HIDE
            if ( ($('#input_tags_hide').val().indexOf("---") >= 0 ) && (tags_ligne_str.indexOf("---") >= 0) ) {
                return false ;  // On n'affiche pas les tags "---" correspondant au "discarded"
            } else {
                // S'il y a des tags pour la ligne, on compare a ceux fixes dans le filtre
                var tags_ligne_arr = tags_ligne_str.split(',') ;
                var tags_a_montrer_arr = $('#input_tags_show').val().split(',') ;

                if ( _.intersection(tags_ligne_arr,tags_a_montrer_arr).length > 0 ) {
                    return true ;
                } else {
                    return false ;
                }
            }
        }
    } );

    // --- Creation de la Datatable
    textaj = "./get_liste?nb_days=" + $('#input_nbdays').val()
    gv_madatatable = $('#tbl_liste').DataTable( {
        "ajax"          : textaj,
        "info"          : true,
        "lengthChange"  : true,
        "paging"        : true,
        "pageLength"    : 15,
        "ordering"      : true,
        "order"         : [[ 0, 'desc' ], [ 1, 'asc' ]],

        "columns"   : [
            { "data": "ts_updated" },
            { "data": "localite" },
            { "data": "price" },
            { "data": "surface" },
            { "data": "title" },
            { "data": "commandes" },
            { "data": "description" }
        ]
    } ) ;
}
