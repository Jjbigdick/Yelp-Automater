from bs4 import BeautifulSoup as bs
import time
import asyncio
import aiohttp
from Yelp.Ship.Ship.logger_config import Logger_config
import json
import csv
import os
from datetime import datetime
import pandas as pa


city_list_tupo = [('New York', 'New York'), ('New York', 'Buffalo'), ('New York', 'Rochester'), ('New York', 'Yonkers'), ('New York', 'Syracuse'), ('New York', 'Albany'), ('New York', 'New Rochelle'), ('New York', 'Mount Vernon'), ('New York', 'Schenectady'), ('New York', 'Utica'), ('New York', 'White Plains'), ('New York', 'Hempstead'), ('New York', 'Troy'), ('New York', 'Niagara Falls'), ('New York', 'Binghamton'), ('New York', 'Freeport'), ('New York', 'Valley Stream'), ('California', 'Los Angeles'), ('California', 'San Diego'), ('California', 'San Jose'), ('California', 'San Francisco'), ('California', 'Fresno'), ('California', 'Sacramento'), ('California', 'Long Beach'), ('California', 'Oakland'), ('California', 'Bakersfield'), ('California', 'Anaheim'), ('California', 'Santa Ana'), ('California', 'Riverside'), ('California', 'Stockton'), ('California', 'Chula Vista'), ('California', 'Irvine'), ('California', 'Fremont'), ('California', 'San Bernardino'), ('California', 'Modesto'), ('California', 'Fontana'), ('California', 'Oxnard'), ('California', 'Moreno Valley'), ('California', 'Huntington Beach'), ('California', 'Glendale'), ('California', 'Santa Clarita'), ('California', 'Garden Grove'), ('California', 'Oceanside'), ('California', 'Rancho Cucamonga'), ('California', 'Santa Rosa'), ('California', 'Ontario'), ('California', 'Lancaster'), ('California', 'Elk Grove'), ('California', 'Corona'), ('California', 'Palmdale'), ('California', 'Salinas'), ('California', 'Pomona'), ('California', 'Hayward'), ('California', 'Escondido'), ('California', 'Torrance'), ('California', 'Sunnyvale'), ('California', 'Orange'), ('California', 'Fullerton'), ('California', 'Pasadena'), ('California', 'Thousand Oaks'), ('California', 'Visalia'), ('California', 'Simi Valley'), ('California', 'Concord'), ('California', 'Roseville'), ('California', 'Victorville'), ('California', 'Santa Clara'), ('California', 'Vallejo'), ('California', 'Berkeley'), ('California', 'El Monte'), ('California', 'Downey'), ('California', 'Costa Mesa'), ('California', 'Inglewood'), ('California', 'Carlsbad'), ('California', 'San Buenaventura (Ventura)'), ('California', 'Fairfield'), ('California', 'West Covina'), ('California', 'Murrieta'), ('California', 'Richmond'), ('California', 'Norwalk'), ('California', 'Antioch'), ('California', 'Temecula'), ('California', 'Burbank'), ('California', 'Daly City'), ('California', 'Rialto'), ('California', 'Santa Maria'), ('California', 'El Cajon'), ('California', 'San Mateo'), ('California', 'Clovis'), ('California', 'Compton'), ('California', 'Jurupa Valley'), ('California', 'Vista'), ('California', 'South Gate'), ('California', 'Mission Viejo'), ('California', 'Vacaville'), ('California', 'Carson'), ('California', 'Hesperia'), ('California', 'Santa Monica'), ('California', 'Westminster'), ('California', 'Redding'), ('California', 'Santa Barbara'), ('California', 'Chico'), ('California', 'Newport Beach'), ('California', 'San Leandro'), ('California', 'San Marcos'), ('California', 'Whittier'), ('California', 'Hawthorne'), ('California', 'Citrus Heights'), ('California', 'Tracy'), ('California', 'Alhambra'), ('California', 'Livermore'), ('California', 'Buena Park'), ('California', 'Menifee'), ('California', 'Hemet'), ('California', 'Lakewood'), ('California', 'Merced'), ('California', 'Chino'), ('California', 'Indio'), ('California', 'Redwood City'), ('California', 'Lake Forest'), ('California', 'Napa'), ('California', 'Tustin'), ('California', 'Bellflower'), ('California', 'Mountain View'), ('California', 'Chino Hills'), ('California', 'Baldwin Park'), ('California', 'Alameda'), ('California', 'Upland'), ('California', 'San Ramon'), ('California', 'Folsom'), ('California', 'Pleasanton'), ('California', 'Union City'), ('California', 'Perris'), ('California', 'Manteca'), ('California', 'Lynwood'), ('California', 'Apple Valley'), ('California', 'Redlands'), ('California', 'Turlock'), ('California', 'Milpitas'), ('California', 'Redondo Beach'), ('California', 'Rancho Cordova'), ('California', 'Yorba Linda'), ('California', 'Palo Alto'), ('California', 'Davis'), ('California', 'Camarillo'), ('California', 'Walnut Creek'), ('California', 'Pittsburg'), ('California', 'South San Francisco'), ('California', 'Yuba City'), ('California', 'San Clemente'), ('California', 'Laguna Niguel'), ('California', 'Pico Rivera'), ('California', 'Montebello'), ('California', 'Lodi'), ('California', 'Madera'), ('California', 'Santa Cruz'), ('California', 'La Habra'), ('California', 'Encinitas'), ('California', 'Monterey Park'), ('California', 'Tulare'), ('California', 'Cupertino'), ('California', 'Gardena'), ('California', 'National City'), ('California', 'Rocklin'), ('California', 'Petaluma'), ('California', 'Huntington Park'), ('California', 'San Rafael'), ('California', 'La Mesa'), ('California', 'Arcadia'), ('California', 'Fountain Valley'), ('California', 'Diamond Bar'), ('California', 'Woodland'), ('California', 'Santee'), ('California', 'Lake Elsinore'), ('California', 'Porterville'), ('California', 'Paramount'), ('California', 'Eastvale'), ('California', 'Rosemead'), ('California', 'Hanford'), ('California', 'Highland'), ('California', 'Brentwood'), ('California', 'Novato'), ('California', 'Colton'), ('California', 'Cathedral City'), ('California', 'Delano'), ('California', 'Yucaipa'), ('California', 'Watsonville'), ('California', 'Placentia'), ('California', 'Glendora'), ('California', 'Gilroy'), ('California', 'Palm Desert'), ('California', 'Cerritos'), ('California', 'West Sacramento'), ('California', 'Aliso Viejo'), ('California', 'Poway'), ('California', 'La Mirada'), ('California', 'Rancho Santa Margarita'), ('California', 'Cypress'), ('California', 'Dublin'), ('California', 'Covina'), ('California', 'Azusa'), ('California', 'Palm Springs'), ('California', 'San Luis Obispo'), ('California', 'Ceres'), ('California', 'San Jacinto'), ('California', 'Lincoln'), ('California', 'Newark'), ('California', 'Lompoc'), ('California', 'El Centro'), ('California', 'Danville'), ('California', 'Bell Gardens'), ('California', 'Coachella'), ('California', 'Rancho Palos Verdes'), ('California', 'San Bruno'), ('California', 'Rohnert Park'), ('California', 'Brea'), ('California', 'La Puente'), ('California', 'Campbell'), ('California', 'San Gabriel'), ('California', 'Beaumont'), ('California', 'Morgan Hill'), ('California', 'Culver City'), ('California', 'Calexico'), ('California', 'Stanton'), ('California', 'La Quinta'), ('California', 'Pacifica'), ('California', 'Montclair'), ('California', 'Oakley'), ('California', 'Monrovia'), ('California', 'Los Banos'), ('California', 'Martinez'), ('Illinois', 'Chicago'), ('Illinois', 'Aurora'), ('Illinois', 'Rockford'), ('Illinois', 'Joliet'), ('Illinois', 'Naperville'), ('Illinois', 'Springfield'), ('Illinois', 'Peoria'), ('Illinois', 'Elgin'), ('Illinois', 'Waukegan'), ('Illinois', 'Cicero'), ('Illinois', 'Champaign'), ('Illinois', 'Bloomington'), ('Illinois', 'Arlington Heights'), ('Illinois', 'Evanston'), ('Illinois', 'Decatur'), ('Illinois', 'Schaumburg'), ('Illinois', 'Bolingbrook'), ('Illinois', 'Palatine'), ('Illinois', 'Skokie'), ('Illinois', 'Des Plaines'), ('Illinois', 'Orland Park'), ('Illinois', 'Tinley Park'), ('Illinois', 'Oak Lawn'), ('Illinois', 'Berwyn'), ('Illinois', 'Mount Prospect'), ('Illinois', 'Normal'), ('Illinois', 'Wheaton'), ('Illinois', 'Hoffman Estates'), ('Illinois', 'Oak Park'), ('Illinois', 'Downers Grove'), ('Illinois', 'Elmhurst'), ('Illinois', 'Glenview'), ('Illinois', 'DeKalb'), ('Illinois', 'Lombard'), ('Illinois', 'Belleville'), ('Illinois', 'Moline'), ('Illinois', 'Buffalo Grove'), ('Illinois', 'Bartlett'), ('Illinois', 'Urbana'), ('Illinois', 'Quincy'), ('Illinois', 'Crystal Lake'), ('Illinois', 'Plainfield'), ('Illinois', 'Streamwood'), ('Illinois', 'Carol Stream'), ('Illinois', 'Romeoville'), ('Illinois', 'Rock Island'), ('Illinois', 'Hanover Park'), ('Illinois', 'Carpentersville'), ('Illinois', 'Wheeling'), ('Illinois', 'Park Ridge'), ('Illinois', 'Addison'), ('Illinois', 'Calumet City'), ('Texas', 'Houston'), ('Texas', 'San Antonio'), ('Texas', 'Dallas'), ('Texas', 'Austin'), ('Texas', 'Fort Worth'), ('Texas', 'El Paso'), ('Texas', 'Arlington'), ('Texas', 'Corpus Christi'), ('Texas', 'Plano'), ('Texas', 'Laredo'), ('Texas', 'Lubbock'), ('Texas', 'Garland'), ('Texas', 'Irving'), ('Texas', 'Amarillo'), ('Texas', 'Grand Prairie'), ('Texas', 'Brownsville'), ('Texas', 'Pasadena'), ('Texas', 'McKinney'), ('Texas', 'Mesquite'), ('Texas', 'McAllen'), ('Texas', 'Killeen'), ('Texas', 'Frisco'), ('Texas', 'Waco'), ('Texas', 'Carrollton'), ('Texas', 'Denton'), ('Texas', 'Midland'), ('Texas', 'Abilene'), ('Texas', 'Beaumont'), ('Texas', 'Round Rock'), ('Texas', 'Odessa'), ('Texas', 'Wichita Falls'), ('Texas', 'Richardson'), ('Texas', 'Lewisville'), ('Texas', 'Tyler'), ('Texas', 'College Station'), ('Texas', 'Pearland'), ('Texas', 'San Angelo'), ('Texas', 'Allen'), ('Texas', 'League City'), ('Texas', 'Sugar Land'), ('Texas', 'Longview'), ('Texas', 'Edinburg'), ('Texas', 'Mission'), ('Texas', 'Bryan'), ('Texas', 'Baytown'), ('Texas', 'Pharr'), ('Texas', 'Temple'), ('Texas', 'Missouri City'), ('Texas', 'Flower Mound'), ('Texas', 'Harlingen'), ('Texas', 'North Richland Hills'), ('Texas', 'Victoria'), ('Texas', 'Conroe'), ('Texas', 'New Braunfels'), ('Texas', 'Mansfield'), ('Texas', 'Cedar Park'), ('Texas', 'Rowlett'), ('Texas', 'Port Arthur'), ('Texas', 'Euless'), ('Texas', 'Georgetown'), ('Texas', 'Pflugerville'), ('Texas', 'DeSoto'), ('Texas', 'San Marcos'), ('Texas', 'Grapevine'), ('Texas', 'Bedford'), ('Texas', 'Galveston'), ('Texas', 'Cedar Hill'), ('Texas', 'Texas City'), ('Texas', 'Wylie'), ('Texas', 'Haltom City'), ('Texas', 'Keller'), ('Texas', 'Coppell'), ('Texas', 'Rockwall'), ('Texas', 'Huntsville'), ('Texas', 'Duncanville'), ('Texas', 'Sherman'), ('Texas', 'The Colony'), ('Texas', 'Burleson'), ('Texas', 'Hurst'), ('Texas', 'Lancaster'), ('Texas', 'Texarkana'), ('Texas', 'Friendswood'), ('Texas', 'Weslaco'), ('Pennsylvania', 'Philadelphia'), ('Pennsylvania', 'Pittsburgh'), ('Pennsylvania', 'Allentown'), ('Pennsylvania', 'Erie'), ('Pennsylvania', 'Reading'), ('Pennsylvania', 'Scranton'), ('Pennsylvania', 'Bethlehem'), ('Pennsylvania', 'Lancaster'), ('Pennsylvania', 'Harrisburg'), ('Pennsylvania', 'Altoona'), ('Pennsylvania', 'York'), ('Pennsylvania', 'State College'), ('Pennsylvania', 'Wilkes-Barre'), ('Arizona', 'Phoenix'), ('Arizona', 'Tucson'), ('Arizona', 'Mesa'), ('Arizona', 'Chandler'), ('Arizona', 'Glendale'), ('Arizona', 'Scottsdale'), ('Arizona', 'Gilbert'), ('Arizona', 'Tempe'), ('Arizona', 'Peoria'), ('Arizona', 'Surprise'), ('Arizona', 'Yuma'), ('Arizona', 'Avondale'), ('Arizona', 'Goodyear'), ('Arizona', 'Flagstaff'), ('Arizona', 'Buckeye'), ('Arizona', 'Lake Havasu City'), ('Arizona', 'Casa Grande'), ('Arizona', 'Sierra Vista'), ('Arizona', 'Maricopa'), ('Arizona', 'Oro Valley'), ('Arizona', 'Prescott'), ('Arizona', 'Bullhead City'), ('Arizona', 'Prescott Valley'), ('Arizona', 'Marana'), ('Arizona', 'Apache Junction'), ('Florida', 'Jacksonville'), ('Florida', 'Miami'), ('Florida', 'Tampa'), ('Florida', 'Orlando'), ('Florida', 'St. Petersburg'), ('Florida', 'Hialeah'), ('Florida', 'Tallahassee'), ('Florida', 'Fort Lauderdale'), ('Florida', 'Port St. Lucie'), ('Florida', 'Cape Coral'), ('Florida', 'Pembroke Pines'), ('Florida', 'Hollywood'), ('Florida', 'Miramar'), ('Florida', 'Gainesville'), ('Florida', 'Coral Springs'), ('Florida', 'Miami Gardens'), ('Florida', 'Clearwater'), ('Florida', 'Palm Bay'), ('Florida', 'Pompano Beach'), ('Florida', 'West Palm Beach'), ('Florida', 'Lakeland'), ('Florida', 'Davie'), ('Florida', 'Miami Beach'), ('Florida', 'Sunrise'), ('Florida', 'Plantation'), ('Florida', 'Boca Raton'), ('Florida', 'Deltona'), ('Florida', 'Largo'), ('Florida', 'Deerfield Beach'), ('Florida', 'Palm Coast'), ('Florida', 'Melbourne'), ('Florida', 'Boynton Beach'), ('Florida', 'Lauderhill'), ('Florida', 'Weston'), ('Florida', 'Fort Myers'), ('Florida', 'Kissimmee'), ('Florida', 'Homestead'), ('Florida', 'Tamarac'), ('Florida', 'Delray Beach'), ('Florida', 'Daytona Beach'), ('Florida', 'North Miami'), ('Florida', 'Wellington'), ('Florida', 'North Port'), ('Florida', 'Jupiter'), ('Florida', 'Ocala'), ('Florida', 'Port Orange'), ('Florida', 'Margate'), ('Florida', 'Coconut Creek'), ('Florida', 'Sanford'), ('Florida', 'Sarasota'), ('Florida', 'Pensacola'), ('Florida', 'Bradenton'), ('Florida', 'Palm Beach Gardens'), ('Florida', 'Pinellas Park'), ('Florida', 'Coral Gables'), ('Florida', 'Doral'), ('Florida', 'Bonita Springs'), ('Florida', 'Apopka'), ('Florida', 'Titusville'), ('Florida', 'North Miami Beach'), ('Florida', 'Oakland Park'), ('Florida', 'Fort Pierce'), ('Florida', 'North Lauderdale'), ('Florida', 'Cutler Bay'), ('Florida', 'Altamonte Springs'), ('Florida', 'St. Cloud'), ('Florida', 'Greenacres'), ('Florida', 'Ormond Beach'), ('Florida', 'Ocoee'), ('Florida', 'Hallandale Beach'), ('Florida', 'Winter Garden'), ('Florida', 'Aventura'), ('Indiana', 'Indianapolis'), ('Indiana', 'Fort Wayne'), ('Indiana', 'Evansville'), ('Indiana', 'South Bend'), ('Indiana', 'Carmel'), ('Indiana', 'Bloomington'), ('Indiana', 'Fishers'), ('Indiana', 'Hammond'), ('Indiana', 'Gary'), ('Indiana', 'Muncie'), ('Indiana', 'Lafayette'), ('Indiana', 'Terre Haute'), ('Indiana', 'Kokomo'), ('Indiana', 'Anderson'), ('Indiana', 'Noblesville'), ('Indiana', 'Greenwood'), ('Indiana', 'Elkhart'), ('Indiana', 'Mishawaka'), ('Indiana', 'Lawrence'), ('Indiana', 'Jeffersonville'), ('Indiana', 'Columbus'), ('Indiana', 'Portage'), ('Ohio', 'Columbus'), ('Ohio', 'Cleveland'), ('Ohio', 'Cincinnati'), ('Ohio', 'Toledo'), ('Ohio', 'Akron'), ('Ohio', 'Dayton'), ('Ohio', 'Parma'), ('Ohio', 'Canton'), ('Ohio', 'Youngstown'), ('Ohio', 'Lorain'), ('Ohio', 'Hamilton'), ('Ohio', 'Springfield'), ('Ohio', 'Kettering'), ('Ohio', 'Elyria'), ('Ohio', 'Lakewood'), ('Ohio', 'Cuyahoga Falls'), ('Ohio', 'Middletown'), ('Ohio', 'Euclid'), ('Ohio', 'Newark'), ('Ohio', 'Mansfield'), ('Ohio', 'Mentor'), ('Ohio', 'Beavercreek'), ('Ohio', 'Cleveland Heights'), ('Ohio', 'Strongsville'), ('Ohio', 'Dublin'), ('Ohio', 'Fairfield'), ('Ohio', 'Findlay'), ('Ohio', 'Warren'), ('Ohio', 'Lancaster'), ('Ohio', 'Lima'), ('Ohio', 'Huber Heights'), ('Ohio', 'Westerville'), ('Ohio', 'Marion'), ('Ohio', 'Grove City'), ('North Carolina', 'Charlotte'), ('North Carolina', 'Raleigh'), ('North Carolina', 'Greensboro'), ('North Carolina', 'Durham'), ('North Carolina', 'Winston-Salem'), ('North Carolina', 'Fayetteville'), ('North Carolina', 'Cary'), ('North Carolina', 'Wilmington'), ('North Carolina', 'High Point'), ('North Carolina', 'Greenville'), ('North Carolina', 'Asheville'), ('North Carolina', 'Concord'), ('North Carolina', 'Gastonia'), ('North Carolina', 'Jacksonville'), ('North Carolina', 'Chapel Hill'), ('North Carolina', 'Rocky Mount'), ('North Carolina', 'Burlington'), ('North Carolina', 'Wilson'), ('North Carolina', 'Huntersville'), ('North Carolina', 'Kannapolis'), ('North Carolina', 'Apex'), ('North Carolina', 'Hickory'), ('North Carolina', 'Goldsboro'), ('Michigan', 'Detroit'), ('Michigan', 'Grand Rapids'), ('Michigan', 'Warren'), ('Michigan', 'Sterling Heights'), ('Michigan', 'Ann Arbor'), ('Michigan', 'Lansing'), ('Michigan', 'Flint'), ('Michigan', 'Dearborn'), ('Michigan', 'Livonia'), ('Michigan', 'Westland'), ('Michigan', 'Troy'), ('Michigan', 'Farmington Hills'), ('Michigan', 'Kalamazoo'), ('Michigan', 'Wyoming'), ('Michigan', 'Southfield'), ('Michigan', 'Rochester Hills'), ('Michigan', 'Taylor'), ('Michigan', 'Pontiac'), ('Michigan', 'St. Clair Shores'), ('Michigan', 'Royal Oak'), ('Michigan', 'Novi'), ('Michigan', 'Dearborn Heights'), ('Michigan', 'Battle Creek'), ('Michigan', 'Saginaw'), ('Michigan', 'Kentwood'), ('Michigan', 'East Lansing'), ('Michigan', 'Roseville'), ('Michigan', 'Portage'), ('Michigan', 'Midland'), ('Michigan', 'Lincoln Park'), ('Michigan', 'Muskegon'), ('Tennessee', 'Memphis'), ('Tennessee', 'Nashville-Davidson'), ('Tennessee', 'Knoxville'), ('Tennessee', 'Chattanooga'), ('Tennessee', 'Clarksville'), ('Tennessee', 'Murfreesboro'), ('Tennessee', 'Jackson'), ('Tennessee', 'Franklin'), ('Tennessee', 'Johnson City'), ('Tennessee', 'Bartlett'), ('Tennessee', 'Hendersonville'), ('Tennessee', 'Kingsport'), ('Tennessee', 'Collierville'), ('Tennessee', 'Cleveland'), ('Tennessee', 'Smyrna'), ('Tennessee', 'Germantown'), ('Tennessee', 'Brentwood'), ('Massachusetts', 'Boston'), ('Massachusetts', 'Worcester'), ('Massachusetts', 'Springfield'), ('Massachusetts', 'Lowell'), ('Massachusetts', 'Cambridge'), ('Massachusetts', 'New Bedford'), ('Massachusetts', 'Brockton'), ('Massachusetts', 'Quincy'), ('Massachusetts', 'Lynn'), ('Massachusetts', 'Fall River'), ('Massachusetts', 'Newton'), ('Massachusetts', 'Lawrence'), ('Massachusetts', 'Somerville'), ('Massachusetts', 'Waltham'), ('Massachusetts', 'Haverhill'), ('Massachusetts', 'Malden'), ('Massachusetts', 'Medford'), ('Massachusetts', 'Taunton'), ('Massachusetts', 'Chicopee'), ('Massachusetts', 'Weymouth Town'), ('Massachusetts', 'Revere'), ('Massachusetts', 'Peabody'), ('Massachusetts', 'Methuen'), ('Massachusetts', 'Barnstable Town'), ('Massachusetts', 'Pittsfield'), ('Massachusetts', 'Attleboro'), ('Massachusetts', 'Everett'), ('Massachusetts', 'Salem'), ('Massachusetts', 'Westfield'), ('Massachusetts', 'Leominster'), ('Massachusetts', 'Fitchburg'), ('Massachusetts', 'Beverly'), ('Massachusetts', 'Holyoke'), ('Massachusetts', 'Marlborough'), ('Massachusetts', 'Woburn'), ('Massachusetts', 'Chelsea'), ('Washington', 'Seattle'), ('Washington', 'Spokane'), ('Washington', 'Tacoma'), ('Washington', 'Vancouver'), ('Washington', 'Bellevue'), ('Washington', 'Kent'), ('Washington', 'Everett'), ('Washington', 'Renton'), ('Washington', 'Yakima'), ('Washington', 'Federal Way'), ('Washington', 'Spokane Valley'), ('Washington', 'Bellingham'), ('Washington', 'Kennewick'), ('Washington', 'Auburn'), ('Washington', 'Pasco'), ('Washington', 'Marysville'), ('Washington', 'Lakewood'), ('Washington', 'Redmond'), ('Washington', 'Shoreline'), ('Washington', 'Richland'), ('Washington', 'Kirkland'), ('Washington', 'Burien'), ('Washington', 'Sammamish'), ('Washington', 'Olympia'), ('Washington', 'Lacey'), ('Washington', 'Edmonds'), ('Washington', 'Bremerton'), ('Washington', 'Puyallup'), ('Colorado', 'Denver'), ('Colorado', 'Colorado Springs'), ('Colorado', 'Aurora'), ('Colorado', 'Fort Collins'), ('Colorado', 'Lakewood'), ('Colorado', 'Thornton'), ('Colorado', 'Arvada'), ('Colorado', 'Westminster'), ('Colorado', 'Pueblo'), ('Colorado', 'Centennial'), ('Colorado', 'Boulder'), ('Colorado', 'Greeley'), ('Colorado', 'Longmont'), ('Colorado', 'Loveland'), ('Colorado', 'Grand Junction'), ('Colorado', 'Broomfield'), ('Colorado', 'Castle Rock'), ('Colorado', 'Commerce City'), ('Colorado', 'Parker'), ('Colorado', 'Littleton'), ('Colorado', 'Northglenn'), ('District of Columbia', 'Washington'), ('Maryland', 'Baltimore'), ('Maryland', 'Frederick'), ('Maryland', 'Rockville'), ('Maryland', 'Gaithersburg'), ('Maryland', 'Bowie'), ('Maryland', 'Hagerstown'), ('Maryland', 'Annapolis'), ('Kentucky', 'Louisville/Jefferson County'), ('Kentucky', 'Lexington-Fayette'), ('Kentucky', 'Bowling Green'), ('Kentucky', 'Owensboro'), ('Kentucky', 'Covington'), ('Oregon', 'Portland'), ('Oregon', 'Eugene'), ('Oregon', 'Salem'), ('Oregon', 'Gresham'), ('Oregon', 'Hillsboro'), ('Oregon', 'Beaverton'), ('Oregon', 'Bend'), ('Oregon', 'Medford'), ('Oregon', 'Springfield'), ('Oregon', 'Corvallis'), ('Oregon', 'Albany'), ('Oregon', 'Tigard'), ('Oregon', 'Lake Oswego'), ('Oregon', 'Keizer'), ('Oklahoma', 'Oklahoma City'), ('Oklahoma', 'Tulsa'), ('Oklahoma', 'Norman'), ('Oklahoma', 'Broken Arrow'), ('Oklahoma', 'Lawton'), ('Oklahoma', 'Edmond'), ('Oklahoma', 'Moore'), ('Oklahoma', 'Midwest City'), ('Oklahoma', 'Enid'), ('Oklahoma', 'Stillwater'), ('Oklahoma', 'Muskogee'), ('Wisconsin', 'Milwaukee'), ('Wisconsin', 'Madison'), ('Wisconsin', 'Green Bay'), ('Wisconsin', 'Kenosha'), ('Wisconsin', 'Racine'), ('Wisconsin', 'Appleton'), ('Wisconsin', 'Waukesha'), ('Wisconsin', 'Eau Claire'), ('Wisconsin', 'Oshkosh'), ('Wisconsin', 'Janesville'), ('Wisconsin', 'West Allis'), ('Wisconsin', 'La Crosse'), ('Wisconsin', 'Sheboygan'), ('Wisconsin', 'Wauwatosa'), ('Wisconsin', 'Fond du Lac'), ('Wisconsin', 'New Berlin'), ('Wisconsin', 'Wausau'), ('Wisconsin', 'Brookfield'), ('Wisconsin', 'Greenfield'), ('Wisconsin', 'Beloit'), ('Nevada', 'Las Vegas'), ('Nevada', 'Henderson'), ('Nevada', 'Reno'), ('Nevada', 'North Las Vegas'), ('Nevada', 'Sparks'), ('Nevada', 'Carson City'), ('New Mexico', 'Albuquerque'), ('New Mexico', 'Las Cruces'), ('New Mexico', 'Rio Rancho'), ('New Mexico', 'Santa Fe'), ('New Mexico', 'Roswell'), ('New Mexico', 'Farmington'), ('New Mexico', 'Clovis'), ('Missouri', 'Kansas City'), ('Missouri', 'St. Louis'), ('Missouri', 'Springfield'), ('Missouri', 'Independence'), ('Missouri', 'Columbia'), ('Missouri', "Lee's Summit"), ('Missouri', "O'Fallon"), ('Missouri', 'St. Joseph'), ('Missouri', 'St. Charles'), ('Missouri', 'St. Peters'), ('Missouri', 'Blue Springs'), ('Missouri', 'Florissant'), ('Missouri', 'Joplin'), ('Missouri', 'Chesterfield'), ('Missouri', 'Jefferson City'), ('Missouri', 'Cape Girardeau'), ('Virginia', 'Virginia Beach'), ('Virginia', 'Norfolk'), ('Virginia', 'Chesapeake'), ('Virginia', 'Richmond'), ('Virginia', 'Newport News'), ('Virginia', 'Alexandria'), ('Virginia', 'Hampton'), ('Virginia', 'Roanoke'), ('Virginia', 'Portsmouth'), ('Virginia', 'Suffolk'), ('Virginia', 'Lynchburg'), ('Virginia', 'Harrisonburg'), ('Virginia', 'Leesburg'), ('Virginia', 'Charlottesville'), ('Virginia', 'Danville'), ('Virginia', 'Blacksburg'), ('Virginia', 'Manassas'), ('Georgia', 'Atlanta'), ('Georgia', 'Columbus'), ('Georgia', 'Augusta-Richmond County'), ('Georgia', 'Savannah'), ('Georgia', 'Athens-Clarke County'), ('Georgia', 'Sandy Springs'), ('Georgia', 'Roswell'), ('Georgia', 'Macon'), ('Georgia', 'Johns Creek'), ('Georgia', 'Albany'), ('Georgia', 'Warner Robins'), ('Georgia', 'Alpharetta'), ('Georgia', 'Marietta'), ('Georgia', 'Valdosta'), ('Georgia', 'Smyrna'), ('Georgia', 'Dunwoody'), ('Nebraska', 'Omaha'), ('Nebraska', 'Lincoln'), ('Nebraska', 'Bellevue'), ('Nebraska', 'Grand Island'), ('Minnesota', 'Minneapolis'), ('Minnesota', 'St. Paul'), ('Minnesota', 'Rochester'), ('Minnesota', 'Duluth'), ('Minnesota', 'Bloomington'), ('Minnesota', 'Brooklyn Park'), ('Minnesota', 'Plymouth'), ('Minnesota', 'St. Cloud'), ('Minnesota', 'Eagan'), ('Minnesota', 'Woodbury'), ('Minnesota', 'Maple Grove'), ('Minnesota', 'Eden Prairie'), ('Minnesota', 'Coon Rapids'), ('Minnesota', 'Burnsville'), ('Minnesota', 'Blaine'), ('Minnesota', 'Lakeville'), ('Minnesota', 'Minnetonka'), ('Minnesota', 'Apple Valley'), ('Minnesota', 'Edina'), ('Minnesota', 'St. Louis Park'), ('Minnesota', 'Mankato'), ('Minnesota', 'Maplewood'), ('Minnesota', 'Moorhead'), ('Minnesota', 'Shakopee'), ('Kansas', 'Wichita'), ('Kansas', 'Overland Park'), ('Kansas', 'Kansas City'), ('Kansas', 'Olathe'), ('Kansas', 'Topeka'), ('Kansas', 'Lawrence'), ('Kansas', 'Shawnee'), ('Kansas', 'Manhattan'), ('Kansas', 'Lenexa'), ('Kansas', 'Salina'), ('Kansas', 'Hutchinson'), ('Louisiana', 'New Orleans'), ('Louisiana', 'Baton Rouge'), ('Louisiana', 'Shreveport'), ('Louisiana', 'Lafayette'), ('Louisiana', 'Lake Charles'), ('Louisiana', 'Kenner'), ('Louisiana', 'Bossier City'), ('Louisiana', 'Monroe'), ('Louisiana', 'Alexandria'), ('Hawaii', 'Honolulu'), ('Alaska', 'Anchorage'), ('New Jersey', 'Newark'), ('New Jersey', 'Jersey City'), ('New Jersey', 'Paterson'), ('New Jersey', 'Elizabeth'), ('New Jersey', 'Clifton'), ('New Jersey', 'Trenton'), ('New Jersey', 'Camden'), ('New Jersey', 'Passaic'), ('New Jersey', 'Union City'), ('New Jersey', 'Bayonne'), ('New Jersey', 'East Orange'), ('New Jersey', 'Vineland'), ('New Jersey', 'New Brunswick'), ('New Jersey', 'Hoboken'), ('New Jersey', 'Perth Amboy'), ('New Jersey', 'West New York'), ('New Jersey', 'Plainfield'), ('New Jersey', 'Hackensack'), ('New Jersey', 'Sayreville'), ('New Jersey', 'Kearny'), ('New Jersey', 'Linden'), ('New Jersey', 'Atlantic City'), ('Idaho', 'Boise City'), ('Idaho', 'Nampa'), ('Idaho', 'Meridian'), ('Idaho', 'Idaho Falls'), ('Idaho', 'Pocatello'), ('Idaho', 'Caldwell'), ('Idaho', "Coeur d'Alene"), ('Idaho', 'Twin Falls'), ('Alabama', 'Birmingham'), ('Alabama', 'Montgomery'), ('Alabama', 'Mobile'), ('Alabama', 'Huntsville'), ('Alabama', 'Tuscaloosa'), ('Alabama', 'Hoover'), ('Alabama', 'Dothan'), ('Alabama', 'Auburn'), ('Alabama', 'Decatur'), ('Alabama', 'Madison'), ('Alabama', 'Florence'), ('Alabama', 'Gadsden'), ('Iowa', 'Des Moines'), ('Iowa', 'Cedar Rapids'), ('Iowa', 'Davenport'), ('Iowa', 'Sioux City'), ('Iowa', 'Iowa City'), ('Iowa', 'Waterloo'), ('Iowa', 'Council Bluffs'), ('Iowa', 'Ames'), ('Iowa', 'West Des Moines'), ('Iowa', 'Dubuque'), ('Iowa', 'Ankeny'), ('Iowa', 'Urbandale'), ('Iowa', 'Cedar Falls'), ('Arkansas', 'Little Rock'), ('Arkansas', 'Fort Smith'), ('Arkansas', 'Fayetteville'), ('Arkansas', 'Springdale'), ('Arkansas', 'Jonesboro'), ('Arkansas', 'North Little Rock'), ('Arkansas', 'Conway'), ('Arkansas', 'Rogers'), ('Arkansas', 'Pine Bluff'), ('Arkansas', 'Bentonville'), ('Utah', 'Salt Lake City'), ('Utah', 'West Valley City'), ('Utah', 'Provo'), ('Utah', 'West Jordan'), ('Utah', 'Orem'), ('Utah', 'Sandy'), ('Utah', 'Ogden'), ('Utah', 'St. George'), ('Utah', 'Layton'), ('Utah', 'Taylorsville'), ('Utah', 'South Jordan'), ('Utah', 'Lehi'), ('Utah', 'Logan'), ('Utah', 'Murray'), ('Utah', 'Draper'), ('Utah', 'Bountiful'), ('Utah', 'Riverton'), ('Utah', 'Roy'), ('Rhode Island', 'Providence'), ('Rhode Island', 'Warwick'), ('Rhode Island', 'Cranston'), ('Rhode Island', 'Pawtucket'), ('Rhode Island', 'East Providence'), ('Rhode Island', 'Woonsocket'), ('Mississippi', 'Jackson'), ('Mississippi', 'Gulfport'), ('Mississippi', 'Southaven'), ('Mississippi', 'Hattiesburg'), ('Mississippi', 'Biloxi'), ('Mississippi', 'Meridian'), ('South Dakota', 'Sioux Falls'), ('South Dakota', 'Rapid City'), ('Connecticut', 'Bridgeport'), ('Connecticut', 'New Haven'), ('Connecticut', 'Stamford'), ('Connecticut', 'Hartford'), ('Connecticut', 'Waterbury'), ('Connecticut', 'Norwalk'), ('Connecticut', 'Danbury'), ('Connecticut', 'New Britain'), ('Connecticut', 'Meriden'), ('Connecticut', 'Bristol'), ('Connecticut', 'West Haven'), ('Connecticut', 'Milford'), ('Connecticut', 'Middletown'), ('Connecticut', 'Norwich'), ('Connecticut', 'Shelton'), ('South Carolina', 'Columbia'), ('South Carolina', 'Charleston'), ('South Carolina', 'North Charleston'), ('South Carolina', 'Mount Pleasant'), ('South Carolina', 'Rock Hill'), ('South Carolina', 'Greenville'), ('South Carolina', 'Summerville'), ('South Carolina', 'Sumter'), ('South Carolina', 'Goose Creek'), ('South Carolina', 'Hilton Head Island'), ('South Carolina', 'Florence'), ('South Carolina', 'Spartanburg'), ('New Hampshire', 'Manchester'), ('New Hampshire', 'Nashua'), ('New Hampshire', 'Concord'), ('North Dakota', 'Fargo'), ('North Dakota', 'Bismarck'), ('North Dakota', 'Grand Forks'), ('North Dakota', 'Minot'), ('Montana', 'Billings'), ('Montana', 'Missoula'), ('Montana', 'Great Falls'), ('Montana', 'Bozeman'), ('Delaware', 'Wilmington'), ('Delaware', 'Dover'), ('Maine', 'Portland'), ('Wyoming', 'Cheyenne'), ('Wyoming', 'Casper'), ('West Virginia', 'Charleston'), ('West Virginia', 'Huntington'), ('Vermont', 'Burlington')]
count = 0

def sort_distinct_print(csv_file_path):
    df = pa.read_csv(csv_file_path)
    df.sort_values(by='timestamp', inplace=True)
    df.drop_duplicates(subset='name', keep='first', inplace=True)
    df.to_csv(csv_file_path, index=False)
    grouped = df.groupby("timestamp")
    max_timestamp = max(grouped, key=lambda x: pa.to_datetime(x[0]))[0]
    max_timestamp_group = grouped.get_group(max_timestamp)
    logger.info(f"Newest group: {max_timestamp}")
    print(max_timestamp_group)

async def write_to_csv(csv_file_path,header, data):
    if not os.path.isfile(csv_file_path):
        with open(csv_file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
    with open(csv_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(data)

async def scrape2(state, city, page, headers):
    url = f"https://www.yelp.com/search?find_desc={business}&find_loc={city}&start={page}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url=url, headers=headers) as response:
                if r:
                html = await response.read()
                await asyncio.sleep(0)
                html = bs(html, "html.parser")
                json_containers_raw = html.find('script', {'type': 'application/json'})
                if json_containers_raw:
                    pure_json = json_containers_raw.text[4:-3]
                    json_ = json.loads(pure_json)
                    try:
                        posts_dic = json_["legacyProps"]["seCuarchAppProps"]["searchPageProps"][
                            "mainContentComponentsListProps"]
                        for post_dic_raw in posts_dic:
                            try:
                                post_dic = post_dic_raw["searchResultBusiness"]
                                try:
                                    name = post_dic["name"]
                                except:
                                    name = None
                                try:
                                    address = post_dic["formattedAddress"]
                                except:
                                    address = None
                                try:
                                    rating = post_dic["rating"]
                                except:
                                    rating = None
                                try:
                                    reviewCount = post_dic["reviewCount"]
                                except:
                                    reviewCount = None
                                try:
                                    url = "https://www.yelp.com" + post_dic["businessUrl"]
                                except:
                                    url = None
                                phone_number = False

                                header = ["state", "city", "name", "address", "rating", "reviewCount",
                                          "profile",
                                          "phone_number", "timestamp"]
                                data = [state, city, name, address, rating, reviewCount, url,
                                        phone_number, timestamp]
                                await write_to_csv(csv_file, header, data)
                            except:
                                # pass this as not post dic
                                pass

                        # logger.info(f"{state} {city} results {page} finished. ")
                        await asyncio.sleep(0.1)
                        return "Finish page."
                    # Found out this page is the end of the limit, terminate this concurrent.
                    except:
                        raise MyCustomException("Page limit reached")

                # Blocked, Reboot this page
                else:
                    logger.critical(f"{state} {city} {page}: IP blocked, rebooting.")
                    await scrape2(state, city, page, headers)

        except MyCustomException:  # Terminate task, as finished.
            global count
            count += 1
            task_timer = time.time()
            logger.info(f"Reach current {state} {city} page limit {count}/{task_len} Duration: {round((task_timer - start)/60)}m")
            await asyncio.sleep(0.1)
            return "Finished city."
        except:
            logger.critical("Proxy down, rebooting proxy.")
            await asyncio.sleep(1)
            await scrape2(state,city,page,headers)


class MyCustomException(Exception):
    pass



async def scrape(tupo):
    async with semaphore:
        proxy_auth = aiohttp.BasicAuth("spgu9st1ei","12345678")
        proxy = f"http://spgu9st1ei:12345678@gate.dc.smartproxy.com:20000"
        state = tupo[0]
        city = tupo[1]
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
            'Accept': 'text/html',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'utf-8',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'TE': 'trailers'}
        for page in range(1,100000,10):
            result = await scrape2(state, city, page, headers)
            if result == "Finished city.":
                return
            else:
                pass




async def Gather(city_list_tupo):
    tasks = []
    for tupo in (city_list_tupo):
        tasks.append(scrape(tupo))
    await asyncio.gather(*tasks)



if __name__ == '__main__':
    business = "home improvement contracting businesses"
    csv_file = f"{business}.csv"
    logger = Logger_config("part1").get_logger()
    timestamp = datetime.now()
    start = time.time()
    task_len = len(city_list_tupo)
    semaphore = asyncio.Semaphore(10)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(Gather(city_list_tupo))


    stop = time.time()
    logger.info(f"{stop - start}")
    sort_distinct_print(csv_file)
    collecting_phone(csv_file,logger)













