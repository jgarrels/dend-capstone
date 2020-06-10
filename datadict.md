## Immigration data

This data comes from https://travel.trade.gov/research/reports/i94/historical/2016.html (US National Tourism and Trade Office).

### Tables

immigration:
- year
- month
- airport_code (international IATA code)
- address (state where the immigrating person will be during their stay)
- visacode (code used by the office) -> visacode_desc.key
- biryear (birthyear of the immigrating person)
- gender (of the immigrating person)
- visatype (the type of visa granted)
- stay_duration (duration of the stay in days)

address_desc:
- key (address code) -> immigration.address
- value (description or explanation of the code)

airport_desc:
- key -> immigration.airport_code
- value

visacode_desc:
- key -> immigration.visacode
- value

## Temperature data

This data comes from Berkeley Earth via Kaggle (https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).

### Table

temperature:
- dt (date)
- avg_temperature (average temperature of the day)
- avg_temperature_uncert (uncertainty of the average temperature)
- city -> airport_codes.municipality, demographics.city
- country
- latitude (of the city)
- longitude (of the city)

## Airport data

This data comes from https://datahub.io/core/airport-codes#data.

### Table

airport_codes
- airport_type
- airport_name
- elevation_ft
- iso_country
- iso_region
- municipality -> demographics.city, temperature.city
- airport_code -> immigration.airport_code
- coordinates

## Demographics data

This data comes from OpenSoft (https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

### Table

demographics:
- city -> airport_codes.municipality, temperature.city
- state
- median_age
- male_population
- female_population
- population
- veterans
- foreign_born
- avg_household_size
- state_code


