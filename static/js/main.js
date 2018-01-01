$(document).ready(function () {
    $('.search_head form').on('submit', function (e) {
        e.preventDefault();
        var search_input = $('.search_head form input');
        if (search_input.val()) {
            $('.loader_div').show();
            sendAjax.post('/search/', {'search': search_input.val()});
            search_input.val('');
        }
    });

    $(document).on('click', '.content_container #main_ > div > div header div', function (e) {
        $('.loader_div').show();
        var id = $(this).data('id');
        sendAjax.post('/sort_entries/', {'id': id});
    });

    $(document).on('click', '.search_result_page h3 a', function (e) {
        $('.search_result_page').removeClass('act');
        $('.entry_container').addClass('act');
    });

    var sendAjax = {

        post: function (url, data) {
            $.ajax
            ({
                type: 'POST',
                url: url,
                data: data,
                dataType: 'json',
                complete: function (json) {
                    return sendAjax.response(url, json);
                }
            });
        },

        response: function (url, data) {
            $('.content_container #main_ > div h3 span').hide();
            var result = data.responseText;

            if (url === '/search/') {
                sendAjax.searchHandler(result);
            }
            else if (url === '/sort_entries/') {
                sendAjax.sortHandler(result);
            }
        },

        searchHandler: function (data) {
            var o = $('.search_result_page');
            $('.entry_container').removeClass('act');
            o.html(data);
            o.addClass('act');
            $('.loader_div').hide();
        },

        sortHandler: function (data) {
            console.log('sort');
            $('.content_container #main_ > div > div').html(data);
            $('.loader_div').hide();
        }
    }

});