import datetime
from datetime import timedelta

from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from django.views.decorators.csrf import csrf_exempt
import requests as req_lib


def home_page_content(request):
    return render(request, 'MSE_Analyzer/home_page_content.html')


def about_us(request):
    return render(request, 'MSE_Analyzer/about-us.html')


def contact(request):
    return render(request, 'MSE_Analyzer/contact.html')


def services(request):

    api_resp_for_all_tickers = req_lib.get("http://flask_backend_revised:5000/all")

    json_all_tickers = api_resp_for_all_tickers.json()

    for elem in json_all_tickers:
        print(elem)
        print('----')

    context = {
        'options_list': json_all_tickers
    }

    template = loader.get_template("MSE_Analyzer/services_intro.html")

    return HttpResponse(template.render(context, request))


@csrf_exempt
def analysed(request):
    if request.method == "POST":
        selected_ticker = request.POST.get('ticker')
        selected_interval = request.POST.get('interval')

        latest_available_date_resp = req_lib.get('http://flask_backend_revised:5000/tickers/latest/str')

        lds_json = latest_available_date_resp.json()

        lds_str = lds_json.strip()

        # Return format: MM/DD/YYYY
        lds_parts = lds_str.split('/')

        interval_end = datetime.datetime(int(lds_parts[2]), int(lds_parts[0]), int(lds_parts[1]))

        if selected_interval == 'last-week':
            interval_start = interval_end - timedelta(days=7)
        elif selected_interval == 'last-month':
            interval_start = interval_end - timedelta(days=30)
        else:
            interval_start = interval_end - timedelta(days=364)

        interval_start_str = interval_start.strftime("%m/%d/%Y")
        interval_end_str = interval_end.strftime("%m/%d/%Y")

        interval_start_with_periods = interval_start_str.replace('/', '.')
        interval_end_with_periods = interval_end_str.replace('/', '.')

        url_endpoint = ("http://flask_backend_revised:5000/tickers/analyze/oscillators/" + interval_start_with_periods + '/' +
                        interval_end_with_periods + '/' + selected_ticker)

        api_resp_for_oscillator_analysis = req_lib.get(url_endpoint)

        json_array_from_oscillator_analysis_response = api_resp_for_oscillator_analysis.json()

        json_array_purged = []

        for dict_elem in json_array_from_oscillator_analysis_response:
            current_dict_elem_replacement = {}
            for key, value in dict_elem.items():
                if "momentum" in key or "date_str" in key:
                    current_dict_elem_replacement[key] = value
            json_array_purged.append(current_dict_elem_replacement)

        template = loader.get_template('MSE_Analyzer/services_graph.html')

        for elem in json_array_purged:
            print(elem)
            print("-----")

        context = {
            'interval_start': interval_start.strftime("%m/%d/%Y"),
            'interval_end': interval_end.strftime("%m/%d/%Y"),
            'ticker_code': selected_ticker,
            'json_array_momentum_oscillators': json_array_purged
        }

        return HttpResponse(template.render(context, request))
