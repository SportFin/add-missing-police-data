from police_api import PoliceAPI

api = PoliceAPI()
api.get_forces()
leicestershire = api.get_force("leicestershire")
city_centre = api.get_neighbourhood(leicestershire, "city-centre-neighbourhood")
print(city_centre.description)
# print(city_centre.contact_details)
# api.get_latest_date()
# api.get_crimes_area(city_centre.boundary)
# len(api.get_crimes_area(city_centre.boundary, date="2013-08"))

# crime = api.get_crimes_area(city_centre.boundary, date="2013-06")[0]
# print(crime.outcomes)
