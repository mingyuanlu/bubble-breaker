$( document ).ready(function() {
    // Smart Tab
    $('#tabs').smartTab({autoProgress: false,stopOnFocus:true,transitionEffect:'vSlide'});
    $( ".btnPreview").on("click", (e)=>{$('#iframe').attr("src",$(e.target).data("url").trim())});
});
function showST(tab_index){
  $('#tabs').smartTab('showTab',tab_index);
}
