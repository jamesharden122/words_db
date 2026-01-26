import os
import io
import zipfile
from requests import post as requests_post, get as requests_get
from datetime import date 
import csv
import itertools
WRDS_BASE = "https://wrds-api.wharton.upenn.edu/data/"
WRDS_TOKEN = "275be14d2410414f1de31176d40e7da3352ab114" # or set env var
HEADERS = {
    "Authorization": f"Token {WRDS_TOKEN}",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
}

OUT_ROOT   = "data/zip/bank_regulatory/holding_company_financials/"

YEAR_FROM  = 1925
YEAR_TO    = date.today().year


def ymd(d): return d.strftime("%Y-%m-%d")

def build_filters(**kv):
    parts=[]
    for k,v in kv.items():
        if v is None: 
            continue
        if k.endswith("__in") and not isinstance(v, str):
            v=",".join(str(x) for x in v)
        parts.append(f"{k}={v}")
    return "(" + "&".join(parts) + ")"

def write_all_pages_to_zip(url, headers, params, zip_path, zip_member_csv_name, fields):
    os.makedirs(os.path.dirname(zip_path), exist_ok=True)
    with zipfile.ZipFile(zip_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        with zf.open(zip_member_csv_name, mode="w") as zipped_fp:
            with io.TextIOWrapper(zipped_fp, encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                next_url, first = url, True
                while next_url:
                    resp = requests_get(next_url, headers=headers, params=params if first else None, timeout=3000)
                    first = False
                    data = resp.json()
                    for row in data.get("results", []):
                        w.writerow({k: row.get(k) for k in fields})
                    next_url = data.get("next")



 

def bhck_series1(): 
    fields = [
        "bhck0010", "bhck0081", "bhck0211", "bhck0213", "bhck0379", "bhck0395", "bhck0397", "bhck0426", "bhck0497", "bhck1226",
        "bhck1227", "bhck1228", "bhck1286", "bhck1287", "bhck1288", "bhck1289", "bhck1290", "bhck1291", "bhck1292", "bhck1293",
        "bhck1294", "bhck1295", "bhck1296", "bhck1297", "bhck1298", "bhck1350", "bhck1410", "bhck1421", "bhck1422", "bhck1423",
        "bhck1545", "bhck1563", "bhck1564", "bhck1583", "bhck1590", "bhck1594", "bhck1597", "bhck1606", "bhck1607", "bhck1608",
        "bhck1611", "bhck1612", "bhck1613", "bhck1615", "bhck1616", "bhck1635", "bhck1636", "bhck1638", "bhck1639", "bhck1651",
        "bhck1698", "bhck1699", "bhck1701", "bhck1702", "bhck1703", "bhck1705", "bhck1706", "bhck1707", "bhck1709", "bhck1710",
        "bhck1711", "bhck1713", "bhck1714", "bhck1715", "bhck1716", "bhck1717", "bhck1718", "bhck1719", "bhck1727", "bhck1731",
        "bhck1732", "bhck1733", "bhck1734", "bhck1735", "bhck1736", "bhck1737", "bhck1738", "bhck1739", "bhck1741", "bhck1742",
        "bhck1743", "bhck1744", "bhck1746", "bhck1752", "bhck1753", "bhck1754", "bhck1755", "bhck1763", "bhck1764", "bhck1766",
        "bhck1773", "bhck1778", "bhck1912", "bhck1913", "bhck1975", "bhck2008", "bhck2011", "bhck2081", "bhck2130", "bhck2143",
        "bhck2148", "bhck2150", "bhck2155", "bhck2160", "bhck2165", "bhck2168", "bhck2182", "bhck2183", "bhck2309", "bhck2332",
        "bhck2333", "bhck2745", "bhck2746", "bhck2747", "bhck2748", "bhck2749", "bhck2750", "bhck2757", "bhck2759", "bhck2769",
        "bhck2771", "bhck2800", "bhck2920", "bhck3000", "bhck3049", "bhck3123", "bhck3124", "bhck3128", "bhck3153", "bhck3163",
        "bhck3164", "bhck3190", "bhck3197", "bhck3215", "bhck3216", "bhck3217", "bhck3230", "bhck3284", "bhck3296", "bhck3297",
        "bhck3298", "bhck3409", "bhck3411", "bhck3430", "bhck3434", "bhck3435", "bhck3450", "bhck3451", "bhck3452", "bhck3492",
        "bhck3493", "bhck3494", "bhck3495", "bhck3499", "bhck3500", "bhck3501", "bhck3502", "bhck3503", "bhck3504", "bhck3505",
        "bhck3506", "bhck3507", "bhck3508", "bhck3522", "bhck3528", "bhck3529", "bhck3530", "bhck3541", "bhck3546", "bhck3571",
        "bhck3572", "bhck3574", "bhck3576", "bhck3578", "bhck3580", "bhck3581", "bhck3582", "bhck3584", "bhck3588", "bhck3590",
        "bhck3656", "bhck3806", "bhck3809", "bhck3812", "bhck3816", "bhck3820", "bhck3822", "bhck3826", "bhck3836", "bhck3837",
        "bhck4010", "bhck4020", "bhck4027", "bhck4042", "bhck4059", "bhck4060", "bhck4065", "bhck4069", "bhck4070", "bhck4074",
        "bhck4078", "bhck4092", "bhck4105", "bhck4106", "bhck4115", "bhck4136", "bhck4141", "bhck4146", "bhck4150", "bhck4172",
        "bhck4180", "bhck4185", "bhck4217", "bhck4219", "bhck4300", "bhck4301", "bhck4302", "bhck4313", "bhck4320", "bhck4336",
        "bhck4340", "bhck4356", "bhck4393", "bhck4394", "bhck4395", "bhck4396", "bhck4397", "bhck4398", "bhck4399", "bhck4411",
        "bhck4412", "bhck4414", "bhck4435", "bhck4436", "bhck4460", "bhck4484", "bhck4503", "bhck4504", "bhck4506", "bhck4507",
        "bhck4518", "bhck4519", "bhck4531", "bhck4574", "bhck4591", "bhck4592", "bhck4598", "bhck4635", "bhck4643", "bhck4644",
        "bhck4645", "bhck4646", "bhck4651", "bhck4652", "bhck4653", "bhck4654", "bhck4655", "bhck4656", "bhck4657", "bhck4658",
        "bhck4659", "bhck4776", "bhck4815", "bhck4832", "bhck4833", "bhck4834", "bhck5041", "bhck5043", "bhck5045", "bhck5047",
        "bhck5310", "bhck5351", "bhck5354", "bhck5355", "bhck5356", "bhck5359", "bhck5360", "bhck5369", "bhck5377", "bhck5378",
        "bhck5379", "bhck5380", "bhck5381", "bhck5382", "bhck5383", "bhck5384", "bhck5385", "bhck5386", "bhck5387", "bhck5388",
        "bhck5389", "bhck5390", "bhck5391", "bhck5393", "bhck5397", "bhck5398", "bhck5399", "bhck5400", "bhck5401", "bhck5402",
        "bhck5403", "bhck5409", "bhck5411", "bhck5413", "bhck5459", "bhck5460", "bhck5461", "bhck5507", "bhck5610", "bhck5612",
        "bhck5613", "bhck5614", "bhck5615", "bhck5616", "bhck5617", "bhck6019", "bhck6373", "bhck6416", "bhck6438", "bhck6441",
        "bhck6442", "bhck6550", "bhck6555", "bhck6556", "bhck6557", "bhck6558", "bhck6559", "bhck6560", "bhck6561", "bhck6566",
        "bhck6572", "bhck6586", "bhck6599", "bhck6649", "bhck6669", "bhck6688", "bhck6689", "bhck6761", "bhck6765", "bhck6927",
        "bhck6928", "bhck6995", "bhck6998", "bhck8403", "bhck8427", "bhck8428", "bhck8429", "bhck8430", "bhck8431", "bhck8433",
        "bhck8434", "bhck8492", "bhck8493", "bhck8494", "bhck8495", "bhck8496", "bhck8497", "bhck8498", "bhck8499", "bhck8531",
        "bhck8532", "bhck8533", "bhck8534", "bhck8535", "bhck8536", "bhck8537", "bhck8538", "bhck8539", "bhck8540", "bhck8541",
        "bhck8542", "bhck8543", "bhck8544", "bhck8545", "bhck8546", "bhck8547", "bhck8548", "bhck8549", "bhck8550", "bhck8557",
        "bhck8558", "bhck8559", "bhck8560", "bhck8561", "bhck8562", "bhck8563", "bhck8564", "bhck8565", "bhck8566", "bhck8567",
        "bhck8693", "bhck8694", "bhck8695", "bhck8696", "bhck8697", "bhck8698", "bhck8699", "bhck8700", "bhck8719", "bhck8720",
        "bhck8733", "bhck8734", "bhck8735", "bhck8736", "bhck8737", "bhck8738", "bhck8739", "bhck8740", "bhck8741", "bhck8742",
        "bhck8743", "bhck8744", "bhck8745", "bhck8746", "bhck8747", "bhck8748", "bhck8749", "bhck8750", "bhck8751", "bhck8752",
        "bhck8753", "bhck8754", "bhck8755", "bhck8756", "bhck8757", "bhck8758", "bhck8759", "bhck8760", "bhck8761", "bhck8762",
        "bhck8763", "bhck8764", "bhck8766", "bhck8767", "bhck8769", "bhck8770", "bhck8771", "bhck8772", "bhck8773", "bhck8774",
        "bhck8775", "bhck8776", "bhck8777", "bhck8778", "bhck8779", "bhck8782", "bhck8783", "bhck8843", "bhcka000", "bhcka001",
        "bhcka002", "bhcka130", "bhcka221", "bhcka222", "bhcka224", "bhcka250", "bhcka251", "bhcka506", "bhcka507", "bhcka510",
        "bhcka511", "bhcka512", "bhcka517", "bhcka518", "bhcka519", "bhcka520", "bhcka521", "bhcka522", "bhcka523", "bhcka524",
        "bhcka525", "bhcka530", "bhcka534", "bhcka535", "bhckb026", "bhckb029", "bhckb030", "bhckb032", "bhckb035", "bhckb036",
        "bhckb039", "bhckb040", "bhckb044", "bhckb045", "bhckb047", "bhckb050", "bhckb051", "bhckb054", "bhckb055", "bhckb077",
        "bhckb488", "bhckb489", "bhckb490", "bhckb492", "bhckb493", "bhckb494", "bhckb496", "bhckb497", "bhckb500", "bhckb501",
        "bhckb502", "bhckb508", "bhckb511", "bhckb512", "bhckb514", "bhckb516", "bhckb522", "bhckb528", "bhckb529", "bhckb530",
        "bhckb538", "bhckb539", "bhckb546", "bhckb556", "bhckb557", "bhckb559", "bhckb560", "bhckb569", "bhckb570", "bhckb572",
        "bhckb573", "bhckb574", "bhckb575", "bhckb576", "bhckb577", "bhckb578", "bhckb579", "bhckb580", "bhckb588", "bhckb590",
        "bhckb591", "bhckb592", "bhckb593", "bhckb594", "bhckb595", "bhckb596", "bhckb639", "bhckb675", "bhckb681", "bhckb747",
        "bhckb748", "bhckb749", "bhckb750", "bhckb751", "bhckb752", "bhckb753", "bhckb761", "bhckb762", "bhckb763", "bhckb770",
        "bhckb771", "bhckb772", "bhckb776", "bhckb777", "bhckb778", "bhckb779", "bhckb780", "bhckb781", "bhckb782", "bhckb790",
        "bhckb791", "bhckb792", "bhckb793", "bhckb794", "bhckb795", "bhckb796", "bhckb797", "bhckb798", "bhckb799", "bhckb800",
        "bhckb801", "bhckb802", "bhckb803", "bhckb806", "bhckb807", "bhckb837", "bhckb838", "bhckb839", "bhckb840", "bhckb841",
        "bhckb842", "bhckb843", "bhckb844", "bhckb845", "bhckb846", "bhckb847", "bhckb848", "bhckb849", "bhckb850", "bhckb851",
        "bhckb852", "bhckb853", "bhckb854", "bhckb855", "bhckb856", "bhckb857", "bhckb858", "bhckb859", "bhckb860", "bhckb861",
        "bhckb983", "bhckb984", "bhckb985", "bhckb986", "bhckb988", "bhckb990", "bhckb991", "bhckb992", "bhckb994", "bhckb996",
        "bhckb998", "bhckc009", "bhckc013", "bhckc014", "bhckc016", "bhckc017", "bhckc050", "bhckc079", "bhckc159", "bhckc160",
        "bhckc161", "bhckc216", "bhckc219", "bhckc220", "bhckc221", "bhckc222", "bhckc225", "bhckc226", "bhckc229", "bhckc230",
        "bhckc231", "bhckc232", "bhckc233", "bhckc234", "bhckc235", "bhckc236", "bhckc237", "bhckc238", "bhckc239", "bhckc240",
        "bhckc241", "bhckc243", "bhckc246", "bhckc250", "bhckc251", "bhckc252", "bhckc253", "bhckc386", "bhckc387", "bhckc390",
        "bhckc410", "bhckc411", "bhckc435", "bhckc447", "bhckc498", "bhckc700", "bhckc701", "bhckc781", "bhckc880", "bhckc884",
        "bhckc886", "bhckc887", "bhckc888", "bhckc889", "bhckc890", "bhckc891", "bhckc892", "bhckc893", "bhckc894", "bhckc895",
        "bhckc896", "bhckc897", "bhckc898", "bhckc968", "bhckc969", "bhckc970", "bhckc971", "bhckc972", "bhckc973", "bhckc974",
        "bhckc975", "bhckc980", "bhckc981", "bhckc982", "bhckc983", "bhckc984", "bhckc985", "bhckc988", "bhckc989", "bhckd958",
        "bhckd959", "bhckd960", "bhckd962", "bhckd963", "bhckd964", "bhckd965", "bhckd967", "bhckd968", "bhckd969", "bhckd970",
        "bhckd971", "bhckd972", "bhckd973", "bhckd974", "bhckd982", "bhckd983", "bhckd984", "bhckd985", "bhckd991", "bhckd992",
        "bhckd993", "bhckd994", "bhckd995", "bhckd996", "bhckf031", "bhckf070", "bhckf071", "bhckf072", "bhckf073", "bhckf158",
        "bhckf159", "bhckf160", "bhckf161", "bhckf162", "bhckf163", "bhckf164", "bhckf165", "bhckf166", "bhckf167", "bhckf168",
        "bhckf169", "bhckf170", "bhckf171", "bhckf172", "bhckf173", "bhckf174", "bhckf175", "bhckf176", "bhckf177", "bhckf178",
        "bhckf179", "bhckf180", "bhckf181", "bhckf182", "bhckf183", "bhckf184", "bhckf185", "bhckf228", "bhckf229", "bhckf241",
        "bhckf242", "bhckf244", "bhckf245", "bhckf247", "bhckf248", "bhckf250", "bhckf251", "bhckf253", "bhckf254", "bhckf256",
        "bhckf257", "bhckf259", "bhckf260", "bhckf262", "bhckf263", "bhckf264", "bhckf465", "bhckf551", "bhckf552", "bhckf553",
        "bhckf554", "bhckf555", "bhckf556", "bhckf557", "bhckf558", "bhckf585", "bhckf586", "bhckf587", "bhckf588", "bhckf589",
        "bhckf608", "bhckf639", "bhckf640", "bhckf655", "bhckf658", "bhckf661", "bhckf662", "bhckf663", "bhckf664", "bhckf665",
        "bhckf666", "bhckf682", "bhckf683", "bhckf684", "bhckf685", "bhckf686", "bhckf687", "bhckf688", "bhckf689", "bhckf690",
        "bhckf691", "bhckf692", "bhckf693", "bhckf694", "bhckf695", "bhckf696", "bhckf697", "bhckf821", "bhckf841", "bhckft28",
        "bhckft29", "bhckft30", "bhckft31", "bhckft32", "bhckft41", "bhckft42", "bhckft43", "bhckft44", "bhckg091", "bhckg092",
        "bhckg093", "bhckg094", "bhckg095", "bhckg096", "bhckg097", "bhckg098", "bhckg099", "bhckg100", "bhckg101", "bhckg102",
        "bhckg103", "bhckg104", "bhckg209", "bhckg210", "bhckg211", "bhckg212", "bhckg213", "bhckg218", "bhckg221", "bhckg234",
        "bhckg235", "bhckg300", "bhckg301", "bhckg302", "bhckg303", "bhckg304", "bhckg305", "bhckg306", "bhckg307", "bhckg308",
        "bhckg309", "bhckg310", "bhckg311", "bhckg312", "bhckg313", "bhckg314", "bhckg315", "bhckg316", "bhckg317", "bhckg318",
        "bhckg319", "bhckg320", "bhckg321", "bhckg322", "bhckg323", "bhckg324", "bhckg325", "bhckg326", "bhckg327", "bhckg328",
        "bhckg329", "bhckg330", "bhckg331", "bhckg336", "bhckg337", "bhckg338", "bhckg339", "bhckg340", "bhckg341", "bhckg342",
        "bhckg343", "bhckg344", "bhckg345", "bhckg346", "bhckg347", "bhckg391", "bhckg392", "bhckg395", "bhckg396", "bhckg401",
        "bhckg402", "bhckg403", "bhckg404", "bhckg405", "bhckg406", "bhckg407", "bhckg408", "bhckg409", "bhckg410", "bhckg411",
        "bhckg412", "bhckg413", "bhckg414", "bhckg415", "bhckg416", "bhckg417", "bhckg474", "bhckg475", "bhckg476", "bhckg477",
        "bhckg478", "bhckg479", "bhckg480", "bhckg481", "bhckg482", "bhckg483", "bhckg484", "bhckg485", "bhckg486", "bhckg487",
        "bhckg488", "bhckg489", "bhckg490", "bhckg491", "bhckg492", "bhckg507", "bhckg508", "bhckg509", "bhckg510", "bhckg511",
        "bhckg521", "bhckg522", "bhckg523", "bhckg524", "bhckg525", "bhckg536", "bhckg537", "bhckg538", "bhckg539", "bhckg540",
        "bhckg541", "bhckg542", "bhckg543", "bhckg544", "bhckg545", "bhckg546", "bhckg547", "bhckg548", "bhckg549", "bhckg550",
        "bhckg561", "bhckg562", "bhckg563", "bhckg564", "bhckg565", "bhckg566", "bhckg567", "bhckg568", "bhckg569", "bhckg570",
        "bhckg571", "bhckg572", "bhckg573", "bhckg574", "bhckg575", "bhckg586", "bhckg587", "bhckg588", "bhckg589", "bhckg590",
        "bhckg597", "bhckg598", "bhckg599", "bhckg600", "bhckg601", "bhckg602", "bhckg606", "bhckg607", "bhckg608", "bhckg609",
        "bhckg610", "bhckg611", "bhckg618", "bhckg619", "bhckg620", "bhckg621", "bhckg622", "bhckg623", "bhckg642", "bhckg804",
        "bhckg805", "bhckg806", "bhckg807", "bhckg808", "bhckg809", "bhckg894", "bhckg914", "bhckh172", "bhckh173", "bhckh174",
        "bhckh175", "bhckh176", "bhckh177", "bhckh178", "bhckh179", "bhckh180", "bhckh181", "bhckh182", "bhckh185", "bhckh186",
        "bhckh187", "bhckh188", "bhckh193", "bhckh194", "bhckh195", "bhckh196", "bhckh197", "bhckh198", "bhckh199", "bhckh200",
        "bhckh270", "bhckh271", "bhckh272", "bhckh273", "bhckh274", "bhckh275", "bhckh276", "bhckh277", "bhckh278", "bhckh279",
        "bhckh280", "bhckh281", "bhckh282", "bhckh283", "bhckh284", "bhckh285", "bhckh286", "bhckh287", "bhckh288", "bhckh293",
        "bhckh294", "bhckh295", "bhckh296", "bhckh297", "bhckh298", "bhckh299", "bhckhj78", "bhckhj79", "bhckhj80", "bhckhj81",
        "bhckhj82", "bhckhj83", "bhckhj84", "bhckhj85", "bhckhj88", "bhckhj89", "bhckhj92", "bhckhj93", "bhckhj94", "bhckhj95",
        "bhckhk03", "bhckhk04", "bhckht58", "bhckht59", "bhckht60", "bhckht61", "bhckht62", "bhckht63", "bhckht64", "bhckht65",
        "bhckht69", "bhckht80", "bhckht83", "bhckht84", "bhckht85", "bhckht87", "bhckht88", "bhckht89", "bhckht91", "bhckht92",
        "bhckht93", "bhckhu09", "bhckhu10", "bhckhu11", "bhckhu12", "bhckhu13", "bhckhu14", "bhckhu15", "bhckhu20", "bhckhu21",
        "bhckhu22", "bhckhu23", "bhckj320", "bhckj447", "bhckj451", "bhckj452", "bhckj453", "bhckj454", "bhckj455", "bhckj456",
        "bhckj461", "bhckj462", "bhckj463", "bhckj536", "bhckj537", "bhckj981", "bhckj982", "bhckj983", "bhckj984", "bhckj985",
        "bhckj986", "bhckj987", "bhckj988", "bhckj989", "bhckj990", "bhckj991", "bhckj992", "bhckj993", "bhckj994", "bhckj995",
        "bhckj996", "bhckj997", "bhckj998", "bhckj999", "bhckja21", "bhckja22", "bhckjf76", "bhckjf84", "bhckjf85", "bhckjf86",
        "bhckjf87", "bhckjf88", "bhckjf89", "bhckjf90", "bhckjf91", "bhckjf92", "bhckjf93", "bhckjh88", "bhckjh91", "bhckjh92",
        "bhckjh93", "bhckjh94", "bhckjh97", "bhckjh98", "bhckjh99", "bhckjj00", "bhckjj01", "bhckjj03", "bhckjj04", "bhckjj05",
        "bhckjj06", "bhckjj07", "bhckjj08", "bhckjj09", "bhckjj11", "bhckjj12", "bhckjj13", "bhckjj14", "bhckjj15", "bhckjj16",
        "bhckjj17", "bhckjj18", "bhckjj19", "bhckjj20", "bhckjj21", "bhckjj23", "bhckjj24", "bhckjj25", "bhckjj26", "bhckjj27",
        "bhckjj28", "bhckjj30", "bhckjj31", "bhckjj32", "bhckjj34", "bhckk001", "bhckk002", "bhckk003", "bhckk004", "bhckk005",
        "bhckk006", "bhckk007", "bhckk008", "bhckk009", "bhckk010", "bhckk011", "bhckk012", "bhckk013", "bhckk014", "bhckk015",
        "bhckk016", "bhckk017", "bhckk018", "bhckk019", "bhckk020", "bhckk021", "bhckk022", "bhckk023", "bhckk024", "bhckk025",
        "bhckk026", "bhckk027", "bhckk028", "bhckk029", "bhckk030", "bhckk031", "bhckk032", "bhckk033", "bhckk034", "bhckk035",
        "bhckk036", "bhckk037", "bhckk038", "bhckk039", "bhckk040", "bhckk041", "bhckk072", "bhckk073", "bhckk074", "bhckk075",
        "bhckk076", "bhckk077", "bhckk078", "bhckk079", "bhckk080", "bhckk081", "bhckk082", "bhckk083", "bhckk084", "bhckk085",
        "bhckk086", "bhckk087", "bhckk088", "bhckk089", "bhckk090", "bhckk091", "bhckk092", "bhckk093", "bhckk094", "bhckk095",
        "bhckk096", "bhckk097", "bhckk098", "bhckk099", "bhckk100", "bhckk101", "bhckk120", "bhckk121", "bhckk122", "bhckk123",
        "bhckk124", "bhckk125", "bhckk126", "bhckk127", "bhckk128", "bhckk129", "bhckk134", "bhckk135", "bhckk136", "bhckk137",
        "bhckk138", "bhckk139", "bhckk140", "bhckk142", "bhckk143", "bhckk144", "bhckk145", "bhckk146", "bhckk147", "bhckk148",
        "bhckk149", "bhckk150", "bhckk151", "bhckk152", "bhckk153", "bhckk154", "bhckk155", "bhckk156", "bhckk157", "bhckk163",
        "bhckk164", "bhckk165", "bhckk167", "bhckk168", "bhckk178", "bhckk179", "bhckk180", "bhckk181", "bhckk182", "bhckk183",
        "bhckk184", "bhckk185", "bhckk186", "bhckk192", "bhckk193", "bhckk194", "bhckk196", "bhckk201", "bhckk202", "bhckk203",
        "bhckk204", "bhckk205", "bhckk207", "bhckk208", "bhckk212", "bhckk213", "bhckk214", "bhckk215", "bhckk216", "bhckk217",
        "bhckk218", "bhckk267", "bhckk269", "bhckk270", "bhckk271", "bhckk272", "bhckk273", "bhckk274", "bhckk275", "bhckk276",
        "bhckk277", "bhckk278", "bhckk279", "bhckk280", "bhckk281", "bhckk282", "bhckk283", "bhckk284", "bhckk285", "bhckk286",
        "bhckk287", "bhckk288", "bhckkx46", "bhckkx47", "bhckkx50", "bhckkx51", "bhckkx52", "bhckkx53", "bhckkx54", "bhckkx55",
        "bhckkx57", "bhckkx58", "bhckkx60", "bhckkx61", "bhckkx62", "bhckkx63", "bhckkx64", "bhckkx65", "bhckky38", "bhcklg24",
        "bhcklg26", "bhckm727", "bhckm728", "bhckm729", "bhckm730", "bhckm731", "bhckm732", "bhckm733", "bhckm734", "bhckm735",
        "bhckm736", "bhckm737", "bhckm738", "bhckm739", "bhckm740", "bhckm741", "bhckm742", "bhckm743", "bhckm744", "bhckm962",
        "bhckmg94", "bhcks396", "bhcks397", "bhcks398", "bhcks399", "bhcks400", "bhcks402", "bhcks403", "bhcks405", "bhcks406",
        "bhcks410", "bhcks411", "bhcks414", "bhcks415", "bhcks416", "bhcks417", "bhcks420", "bhcks421", "bhcks424", "bhcks425",
        "bhcks426", "bhcks427", "bhcks428", "bhcks429", "bhcks432", "bhcks433", "bhcks434", "bhcks435", "bhcks436", "bhcks437",
        "bhcks440", "bhcks441", "bhcks442", "bhcks443", "bhcks446", "bhcks447", "bhcks450", "bhcks451", "bhcks452", "bhcks453",
        "bhcks454", "bhcks455", "bhcks458", "bhcks459", "bhcks460", "bhcks461", "bhcks462", "bhcks463", "bhcks469", "bhcks470",
        "bhcks471", "bhcks476", "bhcks477", "bhcks478", "bhcks479", "bhcks481", "bhcks482", "bhcks483", "bhcks484", "bhcks486",
        "bhcks487", "bhcks488", "bhcks489", "bhcks491", "bhcks492", "bhcks493", "bhcks494", "bhcks496", "bhcks497", "bhcks498",
        "bhcks499", "bhcks511", "bhcks513", "bhcks524", "bhcks549", "bhcks550", "bhcks551", "bhcks552", "bhcks554", "bhcks555",
        "bhcks556", "bhcks557", "bhcks582", "bhcks583", "bhcks584", "bhcks585", "bhcks586", "bhcks587", "bhcks588", "bhcks589",
        "bhcks590", "bhcks591", "bhcks592", "bhcks593", "bhcks594", "bhcks595", "bhcks596", "bhcks597", "bhcks598", "bhcks599",
        "bhcks600", "bhcks601", "bhcks602", "bhcks603", "bhcks604", "bhcks605", "bhcks606", "bhcks607", "bhcks608", "bhcks609",
        "bhcks610", "bhcks611", "bhcks612", "bhcks613", "bhcks614", "bhcks615", "bhcks616", "bhcks617", "bhcks618", "bhcks619",
        "bhcks620", "bhcks621", "bhcks622", "bhcks623", "bhckt047", "bhcky923", "bhcky924", "rssd9001", "rssd9017", "rssd9999",
        "wrdsdownloaddate",
    ]
    time_frame = [(date(y, 1, 1).strftime("%Y-%m-%d"),date(y+10, 1,1).strftime("%Y-%m-%d")) for y in range(2000, 2020,10)]
    time_frame = list(itertools.chain.from_iterable(time_frame)) 
    filters = []
    for i in range(1,len(time_frame)):
        filters.append(build_filters(rssd9999__gte=time_frame[i-1], rssd9999__lte=time_frame[i]))
    filters.append(build_filters(rssd9999__gte=date(2020, 1, 1).strftime("%Y-%m-%d")))

    for flt in filters:
        params = {'filters': flt, 'limit': 1000}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE+"bank.wrds_holding_bhck_1/",
            headers=HEADERS,
            params=params,
            zip_path="../../"+OUT_ROOT+"bhck_series1/"+flt+"_bhck_series1.zip",
            zip_member_csv_name = flt+"_bhck_series1.csv",
            fields=fields
        )


def bhck_series2():
    fields = [
        "bhck0383", "bhck0384", "bhck0387", "bhck0416", "bhck0535", "bhck1395", "bhck1403", "bhck1406", "bhck1407", "bhck1658",
        "bhck1659", "bhck1661", "bhck1771", "bhck1772", "bhck1914", "bhck2033", "bhck2079", "bhck2122", "bhck2123", "bhck2125",
        "bhck2145", "bhck2170", "bhck2221", "bhck2419", "bhck2432", "bhck2635", "bhck2744", "bhck2948", "bhck3196", "bhck3210",
        "bhck3240", "bhck3247", "bhck3283", "bhck3290", "bhck3293", "bhck3300", "bhck3353", "bhck3365", "bhck3368", "bhck3376",
        "bhck3377", "bhck3378", "bhck3401", "bhck3402", "bhck3404", "bhck3408", "bhck3428", "bhck3429", "bhck3432", "bhck3433",
        "bhck3459", "bhck3515", "bhck3516", "bhck3517", "bhck3519", "bhck3521", "bhck3531", "bhck3532", "bhck3533", "bhck3534",
        "bhck3535", "bhck3536", "bhck3537", "bhck3542", "bhck3543", "bhck3545", "bhck3547", "bhck3548", "bhck3573", "bhck3575",
        "bhck3577", "bhck3579", "bhck3583", "bhck3585", "bhck3589", "bhck3591", "bhck3792", "bhck3814", "bhck3815", "bhck3817",
        "bhck3818", "bhck4062", "bhck4073", "bhck4079", "bhck4093", "bhck4107", "bhck4135", "bhck4230", "bhck4243", "bhck4307",
        "bhck4483", "bhck4505", "bhck4605", "bhck4617", "bhck4618", "bhck4627", "bhck4628", "bhck4661", "bhck4662", "bhck4663",
        "bhck4664", "bhck4665", "bhck4666", "bhck4667", "bhck4668", "bhck4669", "bhck4782", "bhck4783", "bhck5306", "bhck5311",
        "bhck5352", "bhck5353", "bhck5357", "bhck5358", "bhck5376", "bhck5396", "bhck5410", "bhck5412", "bhck5414", "bhck5479",
        "bhck5483", "bhck5484", "bhck5500", "bhck5501", "bhck5502", "bhck5503", "bhck5504", "bhck5505", "bhck5523", "bhck5524",
        "bhck5525", "bhck5526", "bhck5990", "bhck6562", "bhck6568", "bhck6570", "bhck6577", "bhck6996", "bhck6997", "bhck7204",
        "bhck7205", "bhck7206", "bhck8274", "bhck8275", "bhck8551", "bhck8552", "bhck8553", "bhck8554", "bhck8555", "bhck8556",
        "bhck8701", "bhck8702", "bhck8703", "bhck8704", "bhck8705", "bhck8706", "bhck8707", "bhck8708", "bhck8709", "bhck8710",
        "bhck8711", "bhck8712", "bhck8713", "bhck8714", "bhck8715", "bhck8716", "bhck8717", "bhck8718", "bhck8723", "bhck8724",
        "bhck8725", "bhck8726", "bhck8727", "bhck8728", "bhck8729", "bhck8730", "bhck8731", "bhck8732", "bhck8765", "bhck8768",
        "bhck8784", "bhck8834", "bhck8836", "bhck8838", "bhck9191", "bhck9802", "bhcka102", "bhcka120", "bhcka121", "bhcka122",
        "bhcka123", "bhcka124", "bhcka126", "bhcka127", "bhcka128", "bhcka195", "bhcka220", "bhcka223", "bhcka249", "bhcka288",
        "bhcka591", "bhckb027", "bhckb028", "bhckb031", "bhckb033", "bhckb034", "bhckb037", "bhckb038", "bhckb041", "bhckb042",
        "bhckb043", "bhckb046", "bhckb048", "bhckb049", "bhckb052", "bhckb053", "bhckb056", "bhckb491", "bhckb507", "bhckb513",
        "bhckb515", "bhckb517", "bhckb541", "bhckb558", "bhckb589", "bhckb696", "bhckb697", "bhckb698", "bhckb699", "bhckb700",
        "bhckb701", "bhckb702", "bhckb703", "bhckb704", "bhckb705", "bhckb706", "bhckb707", "bhckb708", "bhckb709", "bhckb710",
        "bhckb711", "bhckb712", "bhckb713", "bhckb714", "bhckb715", "bhckb716", "bhckb717", "bhckb718", "bhckb719", "bhckb720",
        "bhckb721", "bhckb722", "bhckb723", "bhckb724", "bhckb725", "bhckb726", "bhckb727", "bhckb728", "bhckb729", "bhckb730",
        "bhckb731", "bhckb732", "bhckb733", "bhckb734", "bhckb735", "bhckb736", "bhckb737", "bhckb738", "bhckb739", "bhckb740",
        "bhckb741", "bhckb742", "bhckb743", "bhckb744", "bhckb745", "bhckb746", "bhckb754", "bhckb755", "bhckb756", "bhckb757",
        "bhckb758", "bhckb759", "bhckb760", "bhckb764", "bhckb765", "bhckb766", "bhckb767", "bhckb768", "bhckb769", "bhckb773",
        "bhckb774", "bhckb775", "bhckb783", "bhckb784", "bhckb785", "bhckb786", "bhckb787", "bhckb788", "bhckb789", "bhckb804",
        "bhckb805", "bhckb808", "bhckb809", "bhckb982", "bhckb989", "bhckb995", "bhckb997", "bhckc015", "bhckc018", "bhckc026",
        "bhckc027", "bhckc217", "bhckc218", "bhckc227", "bhckc242", "bhckc244", "bhckc245", "bhckc247", "bhckc248", "bhckc249",
        "bhckc388", "bhckc389", "bhckc391", "bhckc393", "bhckc394", "bhckc395", "bhckc396", "bhckc397", "bhckc398", "bhckc399",
        "bhckc400", "bhckc401", "bhckc402", "bhckc403", "bhckc404", "bhckc405", "bhckc406", "bhckc407", "bhckc408", "bhckc409",
        "bhckc502", "bhckc699", "bhckc779", "bhckc780", "bhckc866", "bhckc867", "bhckc868", "bhckd957", "bhckd961", "bhckd966",
        "bhckd976", "bhckd977", "bhckd978", "bhckd979", "bhckd980", "bhckd981", "bhckd987", "bhckd988", "bhckd989", "bhckd990",
        "bhckd997", "bhckd998", "bhckd999", "bhckf064", "bhckf065", "bhckf066", "bhckf067", "bhckf068", "bhckf069", "bhckf186",
        "bhckf187", "bhckf188", "bhckf230", "bhckf231", "bhckf232", "bhckf240", "bhckf243", "bhckf246", "bhckf249", "bhckf252",
        "bhckf255", "bhckf258", "bhckf261", "bhckf559", "bhckf597", "bhckf598", "bhckf599", "bhckf600", "bhckf601", "bhckf609",
        "bhckf610", "bhckf614", "bhckf615", "bhckf616", "bhckf617", "bhckf618", "bhckf624", "bhckf632", "bhckf633", "bhckf634",
        "bhckf635", "bhckf636", "bhckf641", "bhckf642", "bhckf643", "bhckf644", "bhckf645", "bhckf646", "bhckf647", "bhckf648",
        "bhckf649", "bhckf650", "bhckf651", "bhckf652", "bhckf653", "bhckf654", "bhckf656", "bhckf657", "bhckf659", "bhckf660",
        "bhckf667", "bhckf668", "bhckf669", "bhckf699", "bhckf790", "bhckf837", "bhckf838", "bhckf842", "bhckft04", "bhckft05",
        "bhckg105", "bhckg214", "bhckg215", "bhckg216", "bhckg217", "bhckg219", "bhckg220", "bhckg222", "bhckg299", "bhckg332",
        "bhckg333", "bhckg334", "bhckg335", "bhckg348", "bhckg349", "bhckg350", "bhckg351", "bhckg352", "bhckg353", "bhckg354",
        "bhckg355", "bhckg356", "bhckg357", "bhckg358", "bhckg359", "bhckg360", "bhckg361", "bhckg362", "bhckg363", "bhckg364",
        "bhckg365", "bhckg366", "bhckg367", "bhckg368", "bhckg369", "bhckg370", "bhckg371", "bhckg372", "bhckg373", "bhckg374",
        "bhckg375", "bhckg378", "bhckg379", "bhckg380", "bhckg381", "bhckg382", "bhckg383", "bhckg384", "bhckg385", "bhckg386",
        "bhckg387", "bhckg388", "bhckg418", "bhckg419", "bhckg420", "bhckg421", "bhckg422", "bhckg423", "bhckg424", "bhckg425",
        "bhckg426", "bhckg427", "bhckg428", "bhckg429", "bhckg430", "bhckg431", "bhckg432", "bhckg433", "bhckg434", "bhckg435",
        "bhckg436", "bhckg437", "bhckg438", "bhckg439", "bhckg440", "bhckg441", "bhckg442", "bhckg443", "bhckg444", "bhckg445",
        "bhckg446", "bhckg447", "bhckg448", "bhckg449", "bhckg450", "bhckg451", "bhckg452", "bhckg453", "bhckg454", "bhckg455",
        "bhckg456", "bhckg457", "bhckg458", "bhckg459", "bhckg460", "bhckg461", "bhckg462", "bhckg493", "bhckg494", "bhckg495",
        "bhckg496", "bhckg497", "bhckg498", "bhckg499", "bhckg500", "bhckg501", "bhckg502", "bhckg503", "bhckg504", "bhckg505",
        "bhckg506", "bhckg512", "bhckg513", "bhckg514", "bhckg515", "bhckg516", "bhckg517", "bhckg518", "bhckg519", "bhckg520",
        "bhckg526", "bhckg527", "bhckg528", "bhckg529", "bhckg530", "bhckg531", "bhckg532", "bhckg533", "bhckg534", "bhckg535",
        "bhckg551", "bhckg552", "bhckg553", "bhckg554", "bhckg555", "bhckg556", "bhckg557", "bhckg558", "bhckg559", "bhckg560",
        "bhckg576", "bhckg577", "bhckg578", "bhckg579", "bhckg580", "bhckg581", "bhckg582", "bhckg583", "bhckg584", "bhckg585",
        "bhckg591", "bhckg603", "bhckg604", "bhckg605", "bhckg612", "bhckg613", "bhckg614", "bhckg615", "bhckg616", "bhckg617",
        "bhckg624", "bhckg625", "bhckg626", "bhckg627", "bhckg628", "bhckg629", "bhckg630", "bhckg631", "bhckg632", "bhckg633",
        "bhckg634", "bhckg635", "bhckg636", "bhckg637", "bhckg641", "bhckg651", "bhckg652", "bhckh171", "bhckh191", "bhckh289",
        "bhckh290", "bhckh291", "bhckh292", "bhckh300", "bhckh301", "bhckh302", "bhckh303", "bhckh304", "bhckh307", "bhckh308",
        "bhckh309", "bhckh310", "bhckhj74", "bhckhj75", "bhckhj76", "bhckhj77", "bhckhj86", "bhckhj87", "bhckhj90", "bhckhj91",
        "bhckhj96", "bhckhj97", "bhckhj98", "bhckhj99", "bhckhk00", "bhckhk01", "bhckhk25", "bhckhk26", "bhckhk27", "bhckhk28",
        "bhckht50", "bhckht51", "bhckht52", "bhckht53", "bhckht66", "bhckht67", "bhckht68", "bhckht70", "bhckht81", "bhckht82",
        "bhckht86", "bhckhu16", "bhckhu17", "bhckhu18", "bhckj319", "bhckj321", "bhckj457", "bhckj458", "bhckj459", "bhckjf77",
        "bhckjf78", "bhckjh89", "bhckjh90", "bhckjh95", "bhckjh96", "bhckjj02", "bhckjj33", "bhckk042", "bhckk043", "bhckk044",
        "bhckk102", "bhckk103", "bhckk104", "bhckk133", "bhckk141", "bhckk195", "bhckk197", "bhckk198", "bhckk199", "bhckk200",
        "bhckk206", "bhckk209", "bhckk210", "bhckk211", "bhckkx48", "bhckkx49", "bhckkx56", "bhckkx59", "bhckkx66", "bhckkx67",
        "bhckkx68", "bhckl183", "bhckl184", "bhckl185", "bhckl186", "bhckl187", "bhckl188", "bhckl191", "bhckl192", "bhckle75",
        "bhcklg25", "bhcklg27", "bhcklg28", "bhckll57", "bhckm288", "bhckm708", "bhckm709", "bhckm710", "bhckm711", "bhckm712",
        "bhckm713", "bhckm714", "bhckm715", "bhckm716", "bhckm717", "bhckm719", "bhckm720", "bhckm721", "bhckm722", "bhckm723",
        "bhckm724", "bhckm725", "bhckm726", "bhckm745", "bhckm746", "bhckm747", "bhckm748", "bhckm749", "bhckm750", "bhckm751",
        "bhckmg93", "bhckmg95", "bhcks413", "bhcks419", "bhcks423", "bhcks431", "bhcks439", "bhcks445", "bhcks449", "bhcks457",
        "bhcks466", "bhcks467", "bhcks475", "bhcks480", "bhcks485", "bhcks490", "bhcks495", "bhcks500", "bhcks503", "bhcks504",
        "bhcks505", "bhcks506", "bhcks507", "bhcks510", "bhcks512", "bhcks514", "bhcks515", "bhcks516", "bhcks517", "bhcks518",
        "bhcks519", "bhcks520", "bhcks521", "bhcks522", "bhcks523", "bhcks525", "bhcks526", "bhcks527", "bhcks528", "bhcks529",
        "bhcks530", "bhcks531", "bhcks539", "bhcks540", "bhcks541", "bhcks542", "bhcks543", "bhcks544", "bhcks545", "bhcks546",
        "bhcks547", "bhcks548", "bhcks558", "bhcks559", "bhcks560", "bhcks561", "bhcks562", "bhcks563", "bhcks564", "bhcks565",
        "bhcks566", "bhcks567", "bhcks568", "bhcks569", "bhcks570", "bhcks571", "bhcks572", "bhcks573", "bhcks574", "bhcks575",
        "bhcks576", "bhcks577", "bhcks578", "bhcks579", "bhcks580", "bhcks581", "bhcks624", "rssd9001", "rssd9017", "rssd9999",
        "wrdsdownloaddate",
    ]

    time_frame = [(date(y, 1, 1).strftime("%Y-%m-%d"), date(y + 10, 1, 1).strftime("%Y-%m-%d")) for y in range(2000, 2020, 10)]
    time_frame = list(itertools.chain.from_iterable(time_frame))
    filters = []
    for i in range(1, len(time_frame)):
        filters.append(build_filters(rssd9999__gte=time_frame[i - 1], rssd9999__lte=time_frame[i]))
    filters.append(build_filters(rssd9999__gte=date(2020, 1, 1).strftime("%Y-%m-%d")))

    for flt in filters:
        params = {"filters": flt, "limit": 1000}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE + "bank.wrds_holding_bhck_2/",
            headers=HEADERS,
            params=params,
            zip_path="../../" + OUT_ROOT + "bhck_series2/" + flt + "_bhck_series2.zip",
            zip_member_csv_name=flt + "_bhck_series2.csv",
            fields=fields,
        )


def bhck_other():
    fields = [
        "bhbc3368", "bhbc3402", "bhbc3516", "bhbc3519", "bhbc4070", "bhbc4073", "bhbc4074", "bhbc4079", "bhbc4091", "bhbc4093",
        "bhbc4094", "bhbc4107", "bhbc4135", "bhbc4218", "bhbc4230", "bhbc4301", "bhbc4302", "bhbc4320", "bhbc4340", "bhbc4421",
        "bhbc4475", "bhbc4484", "bhbc4519", "bhbc6061", "bhbca220", "bhbcb490", "bhbcb491", "bhbcb493", "bhbcb494", "bhbcc216",
        "bhbcjj33", "bhc00010", "bhc00390", "bhc01350", "bhc01754", "bhc01773", "bhc02122", "bhc02170", "bhc03411", "bhc03429",
        "bhc03433", "bhc03545", "bhc05369", "bhc06551", "bhc06563", "bhc06566", "bhc06570", "bhc06572", "bhc06574", "bhc06575",
        "bhc06598", "bhc06601", "bhc06602", "bhc06603", "bhc0a167", "bhc0a250", "bhc0b528", "bhc0b546", "bhc0b639", "bhc0b675",
        "bhc0b681", "bhc0c225", "bhc0g591", "bhc20010", "bhc20390", "bhc21350", "bhc21754", "bhc21773", "bhc22122", "bhc22170",
        "bhc23411", "bhc23429", "bhc23433", "bhc23545", "bhc25369", "bhc26551", "bhc26563", "bhc26566", "bhc26570", "bhc26572",
        "bhc26574", "bhc26575", "bhc26598", "bhc26601", "bhc26602", "bhc26603", "bhc2a167", "bhc2a250", "bhc2b528", "bhc2b546",
        "bhc2b639", "bhc2b675", "bhc2b681", "bhc2c225", "bhc2g591", "bhc50390", "bhc51350", "bhc51754", "bhc51773", "bhc52122",
        "bhc52170", "bhc53411", "bhc53433", "bhc53545", "bhc55369", "bhc56551", "bhc56563", "bhc56566", "bhc56570", "bhc56572",
        "bhc56574", "bhc56575", "bhc56598", "bhc56602", "bhc56603", "bhc5a167", "bhc5a250", "bhc5b528", "bhc5b546", "bhc5b639",
        "bhc5b675", "bhc5b681", "bhc5g591", "bhc90010", "bhc90390", "bhc91350", "bhc91727", "bhc91754", "bhc91773", "bhc92122",
        "bhc92170", "bhc93411", "bhc93429", "bhc93433", "bhc93545", "bhc95369", "bhc96551", "bhc96563", "bhc96566", "bhc96570",
        "bhc96572", "bhc96574", "bhc96575", "bhc96598", "bhc96602", "bhc96603", "bhc9a250", "bhc9b528", "bhc9b541", "bhc9b546",
        "bhc9b639", "bhc9b675", "bhc9b681", "bhc9c225", "bhc9g591", "bhca2170", "bhca3792", "bhca5310", "bhca5311", "bhca7204",
        "bhca7205", "bhca7206", "bhca8274", "bhcaa223", "bhcaa224", "bhcab530", "bhcab596", "bhcah036", "bhcah311", "bhcah312",
        "bhcah313", "bhcah314", "bhcajj29", "bhcakw00", "bhcakw03", "bhcakx77", "bhcakx78", "bhcakx79", "bhcakx80", "bhcakx81",
        "bhcakx82", "bhcakx83", "bhcalb58", "bhcalb59", "bhcalb60", "bhcalb61", "bhcale74", "bhcale85", "bhcale86", "bhcale87",
        "bhcale88", "bhcale89", "bhcale90", "bhcale91", "bhcale92", "bhcalf21", "bhcalf22", "bhcalf23", "bhcalf24", "bhcalf25",
        "bhcalf27", "bhcalf28", "bhcamk66", "bhcamk76", "bhcamk77", "bhcamk78", "bhcanc99", "bhcap742", "bhcap793", "bhcap838",
        "bhcap839", "bhcap840", "bhcap841", "bhcap842", "bhcap843", "bhcap844", "bhcap845", "bhcap846", "bhcap847", "bhcap848",
        "bhcap849", "bhcap850", "bhcap851", "bhcap852", "bhcap853", "bhcap854", "bhcap855", "bhcap856", "bhcap857", "bhcap858",
        "bhcap859", "bhcap860", "bhcap861", "bhcap862", "bhcap863", "bhcap864", "bhcap865", "bhcap866", "bhcap867", "bhcap868",
        "bhcap870", "bhcap872", "bhcap875", "bhcaq257", "bhcaq258", "bhcas540", "bhcb2210", "bhcb2389", "bhcb2604", "bhcb3187",
        "bhcb6648", "bhcbhk29", "bhcbj474", "bhce0010", "bhce1727", "bhce1754", "bhce1773", "bhce2170", "bhce3123", "bhce3411",
        "bhce3429", "bhce3433", "bhce3545", "bhce5369", "bhce6566", "bhce6570", "bhce6572", "bhcea167", "bhcea250", "bhceb528",
        "bhceb541", "bhceb546", "bhceb639", "bhceb675", "bhceb681", "bhceg591", "bhcm3531", "bhcm3532", "bhcm3533", "bhcm3534",
        "bhcm3535", "bhcm3536", "bhcm3537", "bhcm3541", "bhcm3543", "bhcp0010", "bhcp0087", "bhcp0201", "bhcp0202", "bhcp0203",
        "bhcp0204", "bhcp0205", "bhcp0206", "bhcp0207", "bhcp0208", "bhcp0209", "bhcp0210", "bhcp0277", "bhcp0279", "bhcp0362",
        "bhcp0363", "bhcp0364", "bhcp0365", "bhcp0368", "bhcp0400", "bhcp0416", "bhcp0447", "bhcp0467", "bhcp0496", "bhcp0508",
        "bhcp0512", "bhcp0515", "bhcp0518", "bhcp0520", "bhcp0522", "bhcp0533", "bhcp0534", "bhcp0536", "bhcp0537", "bhcp0538",
        "bhcp0539", "bhcp0540", "bhcp0541", "bhcp0542", "bhcp0543", "bhcp1273", "bhcp1274", "bhcp1275", "bhcp1276", "bhcp1277",
        "bhcp1278", "bhcp1279", "bhcp1299", "bhcp1403", "bhcp1407", "bhcp1616", "bhcp2123", "bhcp2125", "bhcp2145", "bhcp2160",
        "bhcp2165", "bhcp2170", "bhcp2200", "bhcp2309", "bhcp2332", "bhcp2792", "bhcp2793", "bhcp2794", "bhcp2796", "bhcp2831",
        "bhcp2930", "bhcp3123", "bhcp3128", "bhcp3147", "bhcp3152", "bhcp3153", "bhcp3156", "bhcp3163", "bhcp3164", "bhcp3165",
        "bhcp3210", "bhcp3230", "bhcp3238", "bhcp3239", "bhcp3240", "bhcp3247", "bhcp3283", "bhcp3290", "bhcp3293", "bhcp3298",
        "bhcp3300", "bhcp3409", "bhcp3513", "bhcp3602", "bhcp3603", "bhcp3604", "bhcp3605", "bhcp3606", "bhcp3607", "bhcp3609",
        "bhcp3611", "bhcp3612", "bhcp3613", "bhcp3614", "bhcp3615", "bhcp3616", "bhcp3617", "bhcp3618", "bhcp3619", "bhcp4000",
        "bhcp4062", "bhcp4073", "bhcp4091", "bhcp4130", "bhcp4135", "bhcp4230", "bhcp4243", "bhcp4250", "bhcp4302", "bhcp4320",
        "bhcp4336", "bhcp4340", "bhcp4485", "bhcp4605", "bhcp4635", "bhcp4647", "bhcp4778", "bhcp5485", "bhcp5486", "bhcp5487",
        "bhcp5488", "bhcp5489", "bhcp5993", "bhcp6552", "bhcp6567", "bhcp6571", "bhcp6573", "bhcp6588", "bhcp6589", "bhcp6590",
        "bhcp6591", "bhcp6592", "bhcp6596", "bhcp6600", "bhcp6604", "bhcp6607", "bhcp6619", "bhcp6649", "bhcp6741", "bhcp6742",
        "bhcp6743", "bhcp6744", "bhcp6758", "bhcp6773", "bhcp6775", "bhcp6791", "bhcp6792", "bhcp6793", "bhcp6794", "bhcp6795",
        "bhcp8434", "bhcp8516", "bhcp8517", "bhcp8518", "bhcp8843", "bhcp9191", "bhcp9802", "bhcpa130", "bhcpb530", "bhcpc254",
        "bhcpc255", "bhcpc427", "bhcpc428", "bhcpc447", "bhcpf229", "bhcpf737", "bhcpf817", "bhcpf818", "bhcpf819", "bhcpf820",
        "bhcpf838", "bhcpf841", "bhcpf842", "bhcpft28", "bhcphk02", "bhcpht69", "bhcpht70", "bhcphu25", "bhcphu26", "bhcpj980",
        "bhcpja22", "bhcpjj33", "bhcpk297", "bhcpky38", "bhcpm962", "bhct0426", "bhct1754", "bhct1773", "bhct2143", "bhct2150",
        "bhct2160", "bhct2170", "bhct2750", "bhct3123", "bhct3190", "bhct3210", "bhct3247", "bhct3368", "bhct3411", "bhct3433",
        "bhct3543", "bhct3545", "bhct3547", "bhct3548", "bhct4230", "bhct4340", "bhct4605", "bhct5369", "bhct5610", "bhct6570",
        "bhcta250", "bhctb528", "bhctb590", "bhctb591", "bhcw3792", "bhcw5310", "bhcw5311", "bhcw7205", "bhcw7206", "bhcwa223",
        "bhcwh311", "bhcwkx78", "bhcwkx83", "bhcwle85", "bhcwle86", "bhcwle87", "bhcwlf23", "bhcwlf24", "bhcwlf25", "bhcwmk66",
        "bhcwp793", "bhcwp851", "bhcwp852", "bhcwp853", "bhcwp854", "bhcwp855", "bhcwp856", "bhcwp857", "bhcwp858", "bhcwp859",
        "bhcwp870", "bhcx1754", "bhcx1773", "bhcx3123", "bhcx3210", "bhcx3368", "bhcx3545", "bhcy1773", "bhcy3123", "bhcyja36",
        "bhdm1288", "bhdm1410", "bhdm1415", "bhdm1420", "bhdm1460", "bhdm1480", "bhdm1545", "bhdm1564", "bhdm1590", "bhdm1635",
        "bhdm1755", "bhdm1766", "bhdm1797", "bhdm1975", "bhdm2081", "bhdm2122", "bhdm2123", "bhdm2165", "bhdm3386", "bhdm3387",
        "bhdm3465", "bhdm3466", "bhdm3516", "bhdm3545", "bhdm3546", "bhdm3547", "bhdm3548", "bhdm5367", "bhdm5368", "bhdm6631",
        "bhdm6636", "bhdma164", "bhdma242", "bhdma243", "bhdmb561", "bhdmb562", "bhdmb987", "bhdmb993", "bhdmf560", "bhdmf576",
        "bhdmf577", "bhdmf578", "bhdmf579", "bhdmf580", "bhdmf581", "bhdmf582", "bhdmf583", "bhdmf584", "bhdmf585", "bhdmf586",
        "bhdmf587", "bhdmf588", "bhdmf589", "bhdmf590", "bhdmf591", "bhdmf592", "bhdmf593", "bhdmf594", "bhdmf595", "bhdmf596",
        "bhdmf597", "bhdmf598", "bhdmf599", "bhdmf600", "bhdmf601", "bhdmf604", "bhdmf605", "bhdmf606", "bhdmf607", "bhdmf611",
        "bhdmf612", "bhdmf613", "bhdmf614", "bhdmf615", "bhdmf616", "bhdmf617", "bhdmf618", "bhdmf624", "bhdmf625", "bhdmf626",
        "bhdmf627", "bhdmf628", "bhdmf629", "bhdmf630", "bhdmf631", "bhdmf632", "bhdmf633", "bhdmf634", "bhdmf635", "bhdmf636",
        "bhdmf639", "bhdmf640", "bhdmf670", "bhdmf671", "bhdmf672", "bhdmf673", "bhdmf674", "bhdmf675", "bhdmf676", "bhdmf677",
        "bhdmf678", "bhdmf679", "bhdmf680", "bhdmf681", "bhdmf724", "bhdmg209", "bhdmg210", "bhdmg211", "bhdmg299", "bhdmg332",
        "bhdmg333", "bhdmg334", "bhdmg335", "bhdmg379", "bhdmg380", "bhdmg381", "bhdmg382", "bhdmg383", "bhdmg384", "bhdmg385",
        "bhdmg386", "bhdmg387", "bhdmg388", "bhdmg651", "bhdmg652", "bhdmhk06", "bhdmhk31", "bhdmhk32", "bhdmj451", "bhdmj454",
        "bhdmk045", "bhdmk046", "bhdmk047", "bhdmk048", "bhdmk049", "bhdmk050", "bhdmk051", "bhdmk052", "bhdmk053", "bhdmk054",
        "bhdmk055", "bhdmk056", "bhdmk057", "bhdmk058", "bhdmk059", "bhdmk060", "bhdmk061", "bhdmk062", "bhdmk063", "bhdmk064",
        "bhdmk065", "bhdmk066", "bhdmk067", "bhdmk068", "bhdmk069", "bhdmk070", "bhdmk071", "bhdmk105", "bhdmk106", "bhdmk107",
        "bhdmk108", "bhdmk109", "bhdmk110", "bhdmk111", "bhdmk112", "bhdmk113", "bhdmk114", "bhdmk115", "bhdmk116", "bhdmk117",
        "bhdmk118", "bhdmk119", "bhdmk130", "bhdmk131", "bhdmk132", "bhdmk158", "bhdmk159", "bhdmk160", "bhdmk161", "bhdmk162",
        "bhdmk166", "bhdmk169", "bhdmk170", "bhdmk171", "bhdmk172", "bhdmk173", "bhdmk174", "bhdmk175", "bhdmk176", "bhdmk177",
        "bhdmk187", "bhdmk188", "bhdmk189", "bhdmk190", "bhdmk191", "bhdmk195", "bhdmk196", "bhdmk197", "bhdmk198", "bhdmk199",
        "bhdmk200", "bhdmk208", "bhdmk209", "bhdmk210", "bhdmk211", "bhdmkx57", "bhfn3360", "bhfn3543", "bhfn6631", "bhfn6636",
        "bhfna245", "bhfnk260", "bhod2389", "bhod2604", "bhod3187", "bhod3189", "bhod6648", "bhodhk29", "bhodj474", "bhpa0365",
        "bhpa4340", "bhpx8901", "bhsp0010", "bhsp0027", "bhsp0087", "bhsp0088", "bhsp0089", "bhsp0201", "bhsp0202", "bhsp0206",
        "bhsp0390", "bhsp0416", "bhsp0447", "bhsp0496", "bhsp0508", "bhsp0523", "bhsp0530", "bhsp1283", "bhsp2111", "bhsp2112",
        "bhsp2122", "bhsp2145", "bhsp2148", "bhsp2170", "bhsp2309", "bhsp2723", "bhsp2724", "bhsp2792", "bhsp2794", "bhsp2796",
        "bhsp2932", "bhsp3049", "bhsp3066", "bhsp3123", "bhsp3148", "bhsp3151", "bhsp3152", "bhsp3153", "bhsp3154", "bhsp3155",
        "bhsp3156", "bhsp3158", "bhsp3166", "bhsp3167", "bhsp3210", "bhsp3230", "bhsp3238", "bhsp3239", "bhsp3247", "bhsp3283",
        "bhsp3300", "bhsp3513", "bhsp3523", "bhsp3524", "bhsp3525", "bhsp3526", "bhsp3527", "bhsp3605", "bhsp3620", "bhsp3621",
        "bhsp4000", "bhsp4073", "bhsp4093", "bhsp4130", "bhsp4250", "bhsp4302", "bhsp4336", "bhsp4340", "bhsp4778", "bhsp5993",
        "bhsp6416", "bhsp6649", "bhsp6796", "bhsp6797", "bhsp8434", "bhsp8516", "bhsp8517", "bhsp8519", "bhsp8520", "bhsp8521",
        "bhsp8522", "bhsp8523", "bhsp8524", "bhsp8525", "bhsp8526", "bhsp8527", "bhsp8528", "bhsp8529", "bhsp8530", "bhsp8843",
        "bhsp9191", "bhsp9802", "bhspa024", "bhspa130", "bhspa530", "bhspb530", "bhspc009", "bhspc159", "bhspc160", "bhspc161",
        "bhspc252", "bhspc253", "bhspc254", "bhspc255", "bhspc256", "bhspc257", "bhspc427", "bhspc428", "bhspc447", "bhspc700",
        "bhspc701", "bhspc702", "bhspc884", "bhspf074", "bhspf075", "bhspf229", "bhspf819", "bhspf820", "bhspf838", "bhspf841",
        "bhspf842", "bhspft28", "bhspft42", "bhspft43", "bhspft44", "bhspg234", "bhspg235", "bhspht69", "bhspht70", "bhspht95",
        "bhspj980", "bhspk141", "bhspky38", "bhspm962", "bhspmz36", "bhspnk60", "bhsx8901", "bhtxf655", "bhtxf656", "bhtxf657",
        "bhtxf658", "bhtxf659", "bhtxf660", "bhtxg546", "bhtxg551", "bhtxg556", "bhtxg561", "bhtxg571", "bhtxg576", "bhtxg581",
        "bhtxg586", "rssd4087", "rssd6191", "rssd9001", "rssd9005", "rssd9007", "rssd9008", "rssd9010", "rssd9014", "rssd9016",
        "rssd9017", "rssd9028", "rssd9029", "rssd9030", "rssd9031", "rssd9032", "rssd9037", "rssd9038", "rssd9039", "rssd9042",
        "rssd9044", "rssd9045", "rssd9046", "rssd9047", "rssd9048", "rssd9049", "rssd9050", "rssd9052", "rssd9053", "rssd9054",
        "rssd9055", "rssd9056", "rssd9059", "rssd9060", "rssd9061", "rssd9101", "rssd9130", "rssd9132", "rssd9138", "rssd9146",
        "rssd9150", "rssd9161", "rssd9170", "rssd9192", "rssd9198", "rssd9200", "rssd9210", "rssd9213", "rssd9216", "rssd9220",
        "rssd9320", "rssd9374", "rssd9375", "rssd9421", "rssd9422", "rssd9424", "rssd9425", "rssd9579", "rssd9950", "rssd9955",
        "rssd9999", "texc3573", "texc3575", "texc6373", "texc6561", "texc6562", "texc6568", "texc6586", "texc6995", "texc6996",
        "texc6997", "texc6998", "texc8520", "texc8521", "texc8522", "texc8523", "texc8524", "texc8525", "texc8557", "texc8558",
        "texc8559", "texc8562", "texc8563", "texc8564", "texc8565", "texc8566", "texc8567", "text3571", "text3573", "text3575",
        "text4769", "text5351", "text5352", "text5353", "text5354", "text5355", "text5356", "text5357", "text5358", "text5359",
        "text5360", "text5485", "text5486", "text5487", "text5488", "text5489", "text5523", "text6373", "text6561", "text6562",
        "text6568", "text6586", "text6995", "text6996", "text6997", "text6998", "text8520", "text8521", "text8522", "text8523",
        "text8524", "text8525", "text8526", "text8527", "text8528", "text8529", "text8530", "text8557", "text8558", "text8559",
        "text8562", "text8563", "text8564", "text8565", "text8566", "text8567", "textb027", "textb028", "textb029", "textb030",
        "textb031", "textb032", "textb033", "textb034", "textb035", "textb036", "textb037", "textb038", "textb039", "textb040",
        "textb041", "textb042", "textb043", "textb044", "textb045", "textb046", "textb047", "textb048", "textb049", "textb050",
        "textb051", "textb052", "textb053", "textb054", "textb055", "textb056", "textc231", "textc490", "textc497", "textc703",
        "textc708", "textc714", "textc715", "textft29", "textft31", "wrdsdownloaddate",
    ]

    time_frame = [(date(y, 1, 1).strftime("%Y-%m-%d"), date(y + 10, 1, 1).strftime("%Y-%m-%d")) for y in range(2000, 2020, 10)]
    time_frame = list(itertools.chain.from_iterable(time_frame))
    filters = []
    for i in range(1, len(time_frame)):
        filters.append(build_filters(rssd9999__gte=time_frame[i - 1], rssd9999__lte=time_frame[i]))
    filters.append(build_filters(rssd9999__gte=date(2020, 1, 1).strftime("%Y-%m-%d")))

    for flt in filters:
        params = {"filters": flt, "limit": 1000}
        print(params)
        write_all_pages_to_zip(
            WRDS_BASE + "bank.wrds_bank_crsp_link/",
            headers=HEADERS,
            params=params,
            zip_path="../../" + OUT_ROOT + "bhck_other/" + flt + "_bhck_other.zip",
            zip_member_csv_name=flt + "_bhck_other.csv",
            fields=fields,
        )

def bhck_crsp_link_file():
    fields = ["dt_end", "dt_start", "inst_type","name","permco", "rssd9001"]
    params = {"limit": 100000}
    print(params)
    write_all_pages_to_zip(
        WRDS_BASE + "bank.wrds_holding_other_1/",
        headers=HEADERS,
        params=params,
        zip_path="../../" + OUT_ROOT + "bhck_crsp_link/" +"bhck_other.zip",
        zip_member_csv_name="bhck_link.csv",
        fields=fields,
    )



if __name__ == "__main__":
    #_ = crsp_daily_securities()
    #_ = csrp_indexes()
    #_ = bhck_series1()
    #_ = bhck_series2()
    #_ = bhck_other()
    _ =  bhck_crsp_link_file()
