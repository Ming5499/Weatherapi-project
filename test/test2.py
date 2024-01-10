from datetime import datetime
import pandas as pd

data = {
    "coord": {"lon": 105.8412, "lat": 21.0245},
    "weather": [{"id": 804, "main": "Clouds", "description": "overcast clouds", "icon": "04n"}],
    "base": "stations",
    "main": {"temp": 291.15, "feels_like": 290.78, "temp_min": 291.15, "temp_max": 291.15, "pressure": 1019, "humidity": 68, "sea_level": 1019, "grnd_level": 1017},
    "visibility": 10000,
    "wind": {"speed": 1.26, "deg": 44, "gust": 1.77},
    "clouds": {"all": 100},
    "dt": 1704304307,
    "sys": {"type": 1, "id": 9308, "country": "VN", "sunrise": 1704324856, "sunset": 1704364060},
    "timezone": 25200,
    "id": 1581130,
    "name": "Hanoi",
    "cod": 200
}


def kelvin_to_celsius(kelvin):
    celsius = kelvin - 273.15
    return celsius


def transform_load_data():
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_celsius(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_celsius(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_celsius(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_celsius(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    # Unix convert to local time
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    # Put in dictionary and Transform
    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (C)": temp_fahrenheit,
        "Feels Like (C)": feels_like_fahrenheit,
        "Minimum Temp (C)": min_temp_fahrenheit,
        "Maximum Temp (C)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind Speed": wind_speed,
        "Time of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    return df_data


resulting_dataframe = transform_load_data()
resulting_dataframe.to_csv('data/current_weather_data_hanoi.csv', index=False)
