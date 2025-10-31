import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import numpy as np

client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
app_name = os.getenv("APP_NAME")
# Note this access token will expire in 2 month need to create one after that from the linkedin developers account
access_token = 'AQVaj5CxUhFmr_AdRxzPm7PH2-iFTjwqhTI0WpqC5VoNwVI5uhVuy5J88qMNR531yhziO7-aLr1d1TVhGeHnYTxmXhATmcCGLgUafHeKl2JoOs55mRyAR8TFjtIRMfP5f-DGaOiuUcKBSCQ3DxcaE_2_okr8GPV6MkmxbVUGIT9gBCJKPTnWKcjSJOGM3U5smf6oGaYCqeJT-WIEXlWkms4Lnnwfffl45FFEujUj33zkjXpUZaNMWDE31CtRxJDEAlH6LWqjempP1EBsm5XQaWFSmfzMyAe3exYkhhsUdKDFwnemWOkVJnoTuf12mBqNZSEYxD0Ht9Xl1THx2846Y8m8sbhtEw'
refresh_token = 'AQUWn4Pipq-uCtVgqJPxRLxGL8g00fYz46_L51CEO9IJoVohLVKaAhhjOXrFkraLwzkHIZeXPNhCWFZL3oCLfCwdmfL4tWLN07hYDYShn5q8Vmg_OpY4sm0EMEpsgm6DMbEWPX22LMjDgYv0E0C0AC7YaKuJLDCa9yDSVLZx3UdZ3kHmIlH0xIdcfOOKyJjr6JSF1JbvbgxuZCdQNJp2jbeIT-MnU9Q4jwtRZXLXA6TinoO42G-5aEpVCw4FoAAhLWigOCKZpPJO4gqVeExixkpC4g0cbIhc9O3RHMXIkc5OisfqDyCrsCAKYLWqA5vYK5kW6tKX2NK5VffR30b-uxHCDS41gA'
headers = {
            'Authorization': f'Bearer {access_token}',
            'Accept': 'application/json',
        }
org_id = os.getenv("ORG_ID")
LinkedIn_Version= os.getenv("LINKEDIN_VERSION")

end_date = datetime.now()
start_date = end_date - timedelta(days=400)
start_epoch = int(start_date.timestamp() * 1000)
end_epoch = int(end_date.timestamp() * 1000)

def get_followers_count(start_epoch,end_epoch): 
    try:
        url = (f"https://api.linkedin.com/v2/organizationalEntityFollowerStatistics"
            f"?q=organizationalEntity"
            f"&organizationalEntity=urn:li:organization:{org_id}"
            f"&timeIntervals.timeGranularityType=DAY"
            f"&timeIntervals.timeRange.start={start_epoch}"
            f"&timeIntervals.timeRange.end={end_epoch}"
            )
        response = requests.get(url, headers=headers)
        data_json = response.json()
        followers_data = []
        for item in data_json['elements']:
            organic = item['followerGains'].get('organicFollowerGain', 0)
            paid = item['followerGains'].get('paidFollowerGain', 0)
            auto_invited = 0
            total = organic + paid + auto_invited
            interval = item.get('timeRange',{})
            start_interval = interval.get('start')
            end_interval = interval.get('end')
            start_date= datetime.fromtimestamp(start_interval / 1000)
            end_date = datetime.fromtimestamp(end_interval/1000)
        
            followers_data.append({
                'Organic Followers': organic,
                'Sponsored Followers': paid,
                'Auto Invited Followers': auto_invited,
                'Total Followers': total,
                # 'Start Date': start_date.date(),
                'Created_at': end_date.date()
            })
    except Exception as e:
        logging.error(e)
    return followers_data

def mapping_of_fields():
    LinkedIn_Version= '202503'
    headers = {
        "Authorization": f"Bearer {access_token}",
        'LinkedIn-Version': f'{LinkedIn_Version}',
        "Accept": "application/json"
    }
    url =f"https://api.linkedin.com/rest/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:{org_id}"
    response = requests.get(url, headers=headers)
    data =response.json()

    # Mapping for seniority
    seniority_mapping = {
        "urn:li:seniority:1": "Unpaid",
        "urn:li:seniority:2": "Training",
        "urn:li:seniority:3": "Entry",
        "urn:li:seniority:4": "Senior",
        "urn:li:seniority:5": "Manager",
        "urn:li:seniority:6": "Director",
        "urn:li:seniority:7": "VP",
        "urn:li:seniority:8": "CXO",
        "urn:li:seniority:9": "Partner",
        "urn:li:seniority:10": "Owner"
    }
    seniority_data = data["elements"][0]["followerCountsBySeniority"]

    #Mapping for industry
    industry_dict = {
        "urn:li:industry:43": "Financial Services",
        "urn:li:industry:96": "IT Services and IT Consulting",
        "urn:li:industry:41": "Banking",
        "urn:li:industry:4": "Software Development",
        "urn:li:industry:11": "Business Consulting and Services",
        "urn:li:industry:47": "Accounting",
        "urn:li:industry:80": "Advertising Services",
        "urn:li:industry:6": "Technology, Information and Internet",
        "urn:li:industry:129": "Capital Markets",
        "urn:li:industry:46": "Investment Management",
        "urn:li:industry:1673": "Credit Intermediation",
        "urn:li:industry:68": "Higher Education",
        "urn:li:industry:69": "Education Administration Programs",
        "urn:li:industry:1999": "Education",
        "urn:li:industry:44": "Real Estate",
        "urn:li:industry:42": "Insurance",
        "urn:li:industry:1725": "Insurance Carriers",
        "urn:li:industry:10": "Legal Services",
        "urn:li:industry:1862": "Marketing Services",
        "urn:li:industry:28": "Entertainment Providers",
        "urn:li:industry:2458": "Data Infrastructure and Analytics",
        "urn:li:industry:126": "Media Production",
        "urn:li:industry:132": "E-Learning Providers",
        "urn:li:industry:118": "Computer and Network Security",
        "urn:li:industry:45": "Investment Banking",
        "urn:li:industry:15": "Pharmaceutical Manufacturing",
        "urn:li:industry:8": "Telecommunications",
        "urn:li:industry:14": "Hospitals and Health Care",
        "urn:li:industry:84": "Information Services",
        "urn:li:industry:27": "Retail",
        "urn:li:industry:1713": "Securities and Commodity Exchanges",
        "urn:li:industry:25": "Manufacturing",
        "urn:li:industry:1750": "Human Resources Services",
        "urn:li:industry:137": "Trusts and Estates",
        "urn:li:industry:104": "Staffing and Recruiting",
        "urn:li:industry:97": "Market Research",
        "urn:li:industry:53": "Motor Vehicle Manufacturing",
        "urn:li:industry:116": "Transportation, Logistics, Supply Chain and Storage",
        "urn:li:industry:3102": "IT System Custom Software Development",
        "urn:li:industry:100": "Non-profit Organizations",
        "urn:li:industry:112": "Appliances, Electrical, and Electronics Manufacturing",
        "urn:li:industry:36": "Broadcast Media Production and Distribution",
        "urn:li:industry:123": "Outsourcing and Offshoring Consulting",
        "urn:li:industry:1285": "Internet Marketplace Platforms",
        "urn:li:industry:9": "Law Practice",
        "urn:li:industry:5": "Computer Networking Products",
        "urn:li:industry:140": "Graphic Design",
        "urn:li:industry:99": "Professional Training and Coaching",
        "urn:li:industry:105": "Design Services",
        "urn:li:industry:3128": "Business Intelligence Platforms",
        "urn:li:industry:135": "Industrial Machinery Manufacturing",
        "urn:li:industry:31": "Hospitality",
        "urn:li:industry:57": "Oil and Gas",
        "urn:li:industry:1916": "Office Administration",
        "urn:li:industry:19": "Retail Apparel and Fashion",
        "urn:li:industry:91": "Consumer Services",
        "urn:li:industry:124": "Wellness and Fitness Services",
        "urn:li:industry:70": "Research Services",
        "urn:li:industry:30": "Travel Arrangements",
        "urn:li:industry:3132": "Internet Publishing",
        "urn:li:industry:34": "Food and Beverage Services",
        "urn:li:industry:106": "Construction",
        "urn:li:industry:48": "Venture Capital and Private Equity Principals",
        "urn:li:industry:1770": "IT System Data Services",
        "urn:li:industry:3106": "Real Estate Agents and Brokers",
        "urn:li:industry:54": "Chemical Manufacturing",
        "urn:li:industry:102": "Strategic Management Services",
        "urn:li:industry:51": "Civil Engineering",
        "urn:li:industry:35": "Movies, Videos, and Sound",
        "urn:li:industry:75": "Writing and Editing",
        "urn:li:industry:103": "Food and Beverage Manufacturing",
        "urn:li:industry:143": "Retail Luxury Goods and Jewelry",
        "urn:li:industry:23": "Government Administration",
        "urn:li:industry:110": "Events Services",
        "urn:li:industry:12": "Biotechnology Research",
        "urn:li:industry:90": "Civic and Social Organizations",
        "urn:li:industry:94": "Airlines and Aviation",
        "urn:li:industry:113": "Online Audio and Video Media",
        "urn:li:industry:60": "Textile Manufacturing",
        "urn:li:industry:98": "Public Relations and Communications Services",
        "urn:li:industry:95": "Maritime Transportation",
        "urn:li:industry:82": "Technology, Information and Media",
        "urn:li:industry:1594": "Book and Periodical Publishing",
        "urn:li:industry:1810": "Professional Services",
        "urn:li:industry:56": "Mining",
        "urn:li:industry:67": "Primary and Secondary Education",
        "urn:li:industry:1743": "Insurance and Employee Benefit Funds",
        "urn:li:industry:3124": "Internet News",
        "urn:li:industry:17": "Data Security Software Products",
        "urn:li:industry:3130": "Medical Equipment Manufacturing",
        "urn:li:industry:49": "Wholesale Building Materials",
        "urn:li:industry:33": "Spectator Sports",
        "urn:li:industry:3242": "Engineering Services",
        "urn:li:industry:55": "Machinery Manufacturing",
        "urn:li:industry:22": "Animation and Post-production",
        "urn:li:industry:127": "Retail Groceries",
        "urn:li:industry:92": "Temporary Help Services",
        "urn:li:industry:1925": "Truck Transportation",
        "urn:li:industry:7": "Semiconductor Manufacturing",
        "urn:li:industry:59": "Utilities"
    }

    industry_data = data["elements"][0]["followerCountsByIndustry"]

    # mapping for Job_Function
    function_dict = {
        "urn:li:function:1": "Finance",
        "urn:li:function:2": "Business Development",
        "urn:li:function:3": "Information Technology",
        "urn:li:function:4": "Engineering",
        "urn:li:function:5": "Operations",
        "urn:li:function:6": "Sales",
        "urn:li:function:7": "Media and Communication",
        "urn:li:function:8": "Arts and Design",
        "urn:li:function:9": "Marketing",
        "urn:li:function:10": "Research",
        "urn:li:function:11": "Accounting",
        "urn:li:function:12": "Human Resources",
        "urn:li:function:13": "Education",
        "urn:li:function:14": "Consulting",
        "urn:li:function:15": "Administrative",
        "urn:li:function:16": "Customer Success and Support",
        "urn:li:function:17": "Program and Project Management",
        "urn:li:function:18": "Product Management",
        "urn:li:function:19": "Legal",
        "urn:li:function:20": "Quality Assurance",
        "urn:li:function:21": "Entrepreneurship",
        "urn:li:function:22": "Community and Social Services",
        "urn:li:function:23": "Healthcare Services",
        "urn:li:function:24": "Military and Protective Services",
        "urn:li:function:25": "Purchasing",
        "urn:li:function:26": "Real Estate"
    }
    job_function_data = data["elements"][0]["followerCountsByFunction"]

    # Mapping for Geographical location
    geo_dict = {
        "urn:li:geo:90009639": "Mumbai Metropolitan Region, India",
        "urn:li:geo:90009626": "Greater Delhi Area, India",
        "urn:li:geo:90009633": "Greater Bengaluru Area, India",
        "urn:li:geo:90009650": "Greater Hyderabad Area, India",
        "urn:li:geo:90009642": "Pune/Pimpri-Chinchwad Area, India",
        "urn:li:geo:90009654": "Greater Kolkata Area, India",
        "urn:li:geo:90009627": "Greater Ahmedabad Area, India",
        "urn:li:geo:90009647": "Greater Chennai Area, India",
        "urn:li:geo:104869687": "Noida, India",
        "urn:li:geo:90009628": "Greater Rajkot Area, India",
        "urn:li:geo:90009640": "Greater Nagpur Area, India",
        "urn:li:geo:90009644": "Greater Jaipur Area, India",
        "urn:li:geo:90009636": "Greater Indore Area, India",
        "urn:li:geo:90009653": "Greater Lucknow Area, India",
        "urn:li:geo:90009634": "Greater Bhopal Area, India",
        "urn:li:geo:90009629": "Greater Surat Area, India",
        "urn:li:geo:90009641": "Greater Nashik Area, India",
        "urn:li:geo:90009630": "Greater Vadodara Area, India",
        "urn:li:geo:90009496": "London Area, United Kingdom",
        "urn:li:geo:90009648": "Greater Coimbatore Area, India",
        "urn:li:geo:90009652": "Greater Allahabad Area, India",
        "urn:li:geo:90009625": "Greater Raipur Area, India",
        "urn:li:geo:110308773": "Pune, India",
        "urn:li:geo:112154036": "Bhubaneswar, India",
        "urn:li:geo:90000070": "New York City Metropolitan Area",
        "urn:li:geo:90009624": "Greater Patna Area, India",
        "urn:li:geo:103291313": "Hong Kong SAR, China",
        "urn:li:geo:90009632": "Dhanbad-Ranchi Area, India",
        "urn:li:geo:100839447": "Faridabad, India",
        "urn:li:geo:90009638": "Greater Aurangabad Area, India",
        "urn:li:geo:90009651": "Greater Agra Area, India",
        "urn:li:geo:90009643": "Amritsar/Ludhiana Area, India",
        "urn:li:geo:90009551": "Greater Toronto Area, Canada",
        "urn:li:geo:114348433": "Kanpur, India",
        "urn:li:geo:90000084": "San Francisco Bay Area",
        "urn:li:geo:102007122": "Cairo, Egypt",
        "urn:li:geo:102454443": "Singapore",
        "urn:li:geo:101594353": "Nasik, India",
        "urn:li:geo:90009623": "Greater Vijayawada District, India",
        "urn:li:geo:107346007": "Panvel, India",
        "urn:li:geo:106940483": "Vasai Virar, India",
        "urn:li:geo:100100512": "Kochi, India",
        "urn:li:geo:90009645": "Greater Jodhpur Area, India",
        "urn:li:geo:90009637": "Greater Jabalpur Area, India",
        "urn:li:geo:104261992": "Udaipur, India",
        "urn:li:geo:90010101": "Jakarta Metropolitan Area, Indonesia",
        "urn:li:geo:106494817": "Greater Gwalior Area, India",
        "urn:li:geo:90009635": "Pimpri Chinchwad, India",
        "urn:li:geo:105607401": "Guwahati, India",
        "urn:li:geo:90010076": "Metro Manila, Philippines",
        "urn:li:geo:100139308": "Chandigarh, India",
        "urn:li:geo:102779754": "Jamshedpur, India",
        "urn:li:geo:90009649": "Ulhasnagar, India",
        "urn:li:geo:104970712": "Greater Madurai Area, India",
        "urn:li:geo:107714860": "Dehradun, India",
        "urn:li:geo:90010187": "Ho Chi Minh City Metropolitan Area, Vietnam",
        "urn:li:geo:90009824": "Greater Dublin, Ireland",
        "urn:li:geo:100205264": "Dubai, United Arab Emirates",
        "urn:li:geo:90000007": "Los Angeles Metropolitan Area",
        "urn:li:geo:90000049": "Greater Boston",
        "urn:li:geo:90009524": "Greater Sydney Area, Australia",
        "urn:li:geo:106447486": "Greater Visakhapatnam Area, India",
        "urn:li:geo:90009622": "Tehran, Iran",
        "urn:li:geo:90000031": "Dallas-Fort Worth Metroplex",
        "urn:li:geo:90009521": "Greater Melbourne Area, Australia",
        "urn:li:geo:90000097": "Washington DC-Baltimore Area",
        "urn:li:geo:100665265": "Kathmandu, Nepal",
        "urn:li:geo:103652652": "Amravati, India",
        "urn:li:geo:103684336": "Patna, India",
        "urn:li:geo:106203786": "Doha, Qatar",
        "urn:li:geo:114181430": "Mangaluru, India",
        "urn:li:geo:100988765": "Kozhikode, India",
        "urn:li:geo:116420287": "Panvel Taluka, India",
        "urn:li:geo:90009631": "Mysore, India",
        "urn:li:geo:101207163": "Greater Srinagar Area, India",
        "urn:li:geo:90009987": "Greater Tokyo Area, Japan",
        "urn:li:geo:90009712": "Berlin Metropolitan Area, Germany",
        "urn:li:geo:90009706": "The Randstad, Netherlands",
        "urn:li:geo:103742390": "Trivandrum, India",
        "urn:li:geo:101339379": "Greater Paris Metropolitan Region, France",
        "urn:li:geo:90000014": "Nairobi County, Kenya",
        "urn:li:geo:90009659": "Greater Chicago Area",
        "urn:li:geo:109045472": "Greater Munich Metropolitan Area, Germany",
        "urn:li:geo:102873056": "Thiruvananthapuram, India",
        "urn:li:geo:90009735": "Mahad, India",
        "urn:li:geo:104381486": "Kolhapur, India",
        "urn:li:geo:90000091": "Kalsi, India",
        "urn:li:geo:105765243": "Riyadh Region, Saudi Arabia",
        "urn:li:geo:90010388": "Greater Seattle Area",
        "urn:li:geo:100032154": "Ajmer, India",
        "urn:li:geo:100835287": "Itanagar, India",
        "urn:li:geo:107102089": "Gorakhpur, India",
        "urn:li:geo:102782588": "Bhubaneshwar, India",
        "urn:li:geo:90010064": "Greater Kuala Lumpur, Malaysia",
        "urn:li:geo:112393311": "Haridwar, India",
        "urn:li:geo:90009574": "Tiruchirappalli, India",
        "urn:li:geo:105304484": "Greater São Paulo Area, Brazil",
        "urn:li:geo:103740787": "Gandhidham, India",
        "urn:li:geo:106287053": "Moradabad, India",
        "urn:li:geo:90000052": "Atlanta Metropolitan Area"
    }

    geo_data = data["elements"][0]["followerCountsByGeo"]

    # Mapping for company size
    company_size_dict = {
        "SIZE_10001_OR_MORE": "10001+",
        "SIZE_51_TO_200": "51-200",
        "SIZE_201_TO_500": "201-500",
        "SIZE_1001_TO_5000": "1001-5000",
        "SIZE_11_TO_50": "11-50",
        "SIZE_501_TO_1000": "501-1000",
        "SIZE_2_TO_10": "2-10",
        "SIZE_5001_TO_10000": "5001-10000",
        "SIZE_1": "1"
    }
    company_size_data = data["elements"][0]["followerCountsByStaffCountRange"]

    seniority_data = [
        {
            "name": seniority_mapping.get(entry["seniority"], entry["seniority"]),
            "total_followers": entry["followerCounts"]["organicFollowerCount"]
        }
        for entry in seniority_data
    ]
    industry_data = [
        {
            "industry": industry_dict.get(entry["industry"], entry["industry"]),
            "total_followers": entry["followerCounts"]["organicFollowerCount"]
        }
        for entry in industry_data
    ]
    function_data = [
        {
            "job_function": function_dict.get(entry["function"], entry["function"]),
            "total_followers": entry["followerCounts"]["organicFollowerCount"]
        }
        for entry in job_function_data
    ]
    geo_data = [
        {
            "geo": geo_dict.get(entry["geo"], entry["geo"]),
            "total_followers": entry["followerCounts"]["organicFollowerCount"]
        }
        for entry in geo_data
    ]
    company_size_data = [
        {
            "company": company_size_dict.get(entry["staffCountRange"], entry["staffCountRange"]),
            "total_followers": entry["followerCounts"]["organicFollowerCount"]
        }
        for entry in company_size_data
    ]
    return seniority_data,industry_data, function_data, geo_data, company_size_data

def linkedin_visitors():

    url = (
        f"https://api.linkedin.com/v2/organizationPageStatistics"
        f"?q=organization"
        f"&organization=urn:li:organization:{org_id}"
        f"&timeIntervals.timeGranularityType=DAY"
        f"&timeIntervals.timeRange.start={start_epoch}"
        f"&timeIntervals.timeRange.end={end_epoch}"
    )
    response = requests.get(url, headers=headers)
    data = response.json()

    def get_nested(d, keys, default=0):
        for key in keys:
            d = d.get(key, {})
        return d if isinstance(d, (int, float)) else default

    visitors_records = []
        
    for elem in data["elements"]:
        views = elem.get("totalPageStatistics", {}).get("views", {})
        time_range = elem.get("timeRange", {})
        start_interval = time_range.get('start')
        end_interval = time_range.get('end')
        data = {
            # Overview
            'Overview_page_views_total': get_nested(views, ['overviewPageViews', 'pageViews']),
            'Overview_page_views_desktop': get_nested(views, ['desktopOverviewPageViews', 'pageViews']),
            'Overview_page_views_mobile': get_nested(views, ['mobileOverviewPageViews', 'pageViews']),
            'Overview_unique_visitors_total': get_nested(views, ['overviewPageViews', 'uniquePageViews']),
            'Overview_unique_visitors_desktop': get_nested(views, ['desktopOverviewPageViews', 'uniquePageViews']),
            'Overview_unique_visitors_mobile': get_nested(views, ['mobileOverviewPageViews', 'uniquePageViews']),

            # Life
            'Life_page_views_total': get_nested(views, ['lifeAtPageViews', 'pageViews']),
            'Life_page_views_desktop': get_nested(views, ['desktopLifeAtPageViews', 'pageViews']),
            'Life_page_views_mobile': get_nested(views, ['mobileLifeAtPageViews', 'pageViews']),
            'Life_unique_visitors_desktop': None,
            'Life_unique_visitors_mobile': None,
            'Life_unique_visitors_total': None,

            # Jobs
            'Jobs_page_views_total': get_nested(views, ['jobsPageViews', 'pageViews']),
            'Jobs_page_views_desktop': get_nested(views, ['desktopJobsPageViews', 'pageViews']),
            'Jobs_page_views_mobile': get_nested(views, ['mobileJobsPageViews', 'pageViews']),
            'Jobs_unique_visitors_total': get_nested(views, ['jobsPageViews', 'uniquePageViews']),
            'Jobs_unique_visitors_desktop': get_nested(views, ['desktopJobsPageViews', 'uniquePageViews']),
            'Jobs_unique_visitors_mobile': get_nested(views, ['mobileJobsPageViews', 'uniquePageViews']),
            
            # Total
            'Total_page_views_total': get_nested(views, ['allPageViews', 'pageViews']),
            'Total_page_views_desktop': get_nested(views, ['allDesktopPageViews', 'pageViews']),
            'Total_page_views_mobile': get_nested(views, ['allMobilePageViews', 'pageViews']),
            'Total_unique_visitors_total': get_nested(views, ['allPageViews', 'uniquePageViews']),
            'Total_unique_visitors_desktop': get_nested(views, ['allDesktopPageViews', 'uniquePageViews']),
            'Total_unique_visitors_mobile': get_nested(views, ['allMobilePageViews', 'uniquePageViews']),
            # Date Range
            # 'Start_Date': datetime.fromtimestamp(start_interval / 1000),
            'Created_at': datetime.fromtimestamp(end_interval / 1000),
        }

        visitors_records.append(data)
        # for record in visitors_records:
        #     print(record['Jobs_unique visitors_total'])

    return visitors_records



