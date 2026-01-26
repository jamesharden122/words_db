use super::super::{AppError, DuckCrudModel, SurrealCrudModel, ToPolars};
use arrow_array::{Array, BooleanArray, Date32Array, Float64Array, StringArray};
use chrono::NaiveDate;
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BhckSeries1 {
    pub bhck0010: Option<f64>,
    pub bhck0081: Option<f64>,
    pub bhck0211: Option<f64>,
    pub bhck0213: Option<f64>,
    pub bhck0379: Option<f64>,
    pub bhck0395: Option<f64>,
    pub bhck0397: Option<f64>,
    pub bhck0426: Option<f64>,
    pub bhck0497: Option<f64>,
    pub bhck1226: Option<f64>,
    pub bhck1227: Option<f64>,
    pub bhck1228: Option<f64>,
    pub bhck1286: Option<f64>,
    pub bhck1287: Option<f64>,
    pub bhck1288: Option<f64>,
    pub bhck1289: Option<f64>,
    pub bhck1290: Option<f64>,
    pub bhck1291: Option<f64>,
    pub bhck1292: Option<f64>,
    pub bhck1293: Option<f64>,
    pub bhck1294: Option<f64>,
    pub bhck1295: Option<f64>,
    pub bhck1296: Option<f64>,
    pub bhck1297: Option<f64>,
    pub bhck1298: Option<f64>,
    pub bhck1350: Option<f64>,
    pub bhck1410: Option<f64>,
    pub bhck1421: Option<bool>,
    pub bhck1422: Option<f64>,
    pub bhck1423: Option<f64>,
    pub bhck1545: Option<f64>,
    pub bhck1563: Option<f64>,
    pub bhck1564: Option<f64>,
    pub bhck1583: Option<f64>,
    pub bhck1590: Option<f64>,
    pub bhck1594: Option<f64>,
    pub bhck1597: Option<f64>,
    pub bhck1606: Option<f64>,
    pub bhck1607: Option<f64>,
    pub bhck1608: Option<f64>,
    pub bhck1611: Option<f64>,
    pub bhck1612: Option<f64>,
    pub bhck1613: Option<f64>,
    pub bhck1615: Option<f64>,
    pub bhck1616: Option<f64>,
    pub bhck1635: Option<f64>,
    pub bhck1636: Option<f64>,
    pub bhck1638: Option<f64>,
    pub bhck1639: Option<f64>,
    pub bhck1651: Option<f64>,
    pub bhck1698: Option<f64>,
    pub bhck1699: Option<f64>,
    pub bhck1701: Option<f64>,
    pub bhck1702: Option<f64>,
    pub bhck1703: Option<f64>,
    pub bhck1705: Option<f64>,
    pub bhck1706: Option<f64>,
    pub bhck1707: Option<f64>,
    pub bhck1709: Option<f64>,
    pub bhck1710: Option<f64>,
    pub bhck1711: Option<f64>,
    pub bhck1713: Option<f64>,
    pub bhck1714: Option<f64>,
    pub bhck1715: Option<f64>,
    pub bhck1716: Option<f64>,
    pub bhck1717: Option<f64>,
    pub bhck1718: Option<f64>,
    pub bhck1719: Option<f64>,
    pub bhck1727: Option<f64>,
    pub bhck1731: Option<f64>,
    pub bhck1732: Option<f64>,
    pub bhck1733: Option<f64>,
    pub bhck1734: Option<f64>,
    pub bhck1735: Option<f64>,
    pub bhck1736: Option<f64>,
    pub bhck1737: Option<f64>,
    pub bhck1738: Option<f64>,
    pub bhck1739: Option<f64>,
    pub bhck1741: Option<f64>,
    pub bhck1742: Option<f64>,
    pub bhck1743: Option<f64>,
    pub bhck1744: Option<f64>,
    pub bhck1746: Option<f64>,
    pub bhck1752: Option<f64>,
    pub bhck1753: Option<f64>,
    pub bhck1754: Option<f64>,
    pub bhck1755: Option<f64>,
    pub bhck1763: Option<f64>,
    pub bhck1764: Option<f64>,
    pub bhck1766: Option<f64>,
    pub bhck1773: Option<f64>,
    pub bhck1778: Option<f64>,
    pub bhck1912: Option<f64>,
    pub bhck1913: Option<f64>,
    pub bhck1975: Option<f64>,
    pub bhck2008: Option<f64>,
    pub bhck2011: Option<f64>,
    pub bhck2081: Option<f64>,
    pub bhck2130: Option<f64>,
    pub bhck2143: Option<f64>,
    pub bhck2148: Option<f64>,
    pub bhck2150: Option<f64>,
    pub bhck2155: Option<f64>,
    pub bhck2160: Option<f64>,
    pub bhck2165: Option<f64>,
    pub bhck2168: Option<f64>,
    pub bhck2182: Option<f64>,
    pub bhck2183: Option<f64>,
    pub bhck2309: Option<f64>,
    pub bhck2332: Option<f64>,
    pub bhck2333: Option<f64>,
    pub bhck2745: Option<f64>,
    pub bhck2746: Option<f64>,
    pub bhck2747: Option<f64>,
    pub bhck2748: Option<f64>,
    pub bhck2749: Option<f64>,
    pub bhck2750: Option<f64>,
    pub bhck2757: Option<f64>,
    pub bhck2759: Option<f64>,
    pub bhck2769: Option<f64>,
    pub bhck2771: Option<f64>,
    pub bhck2800: Option<f64>,
    pub bhck2920: Option<f64>,
    pub bhck3000: Option<f64>,
    pub bhck3049: Option<f64>,
    pub bhck3123: Option<f64>,
    pub bhck3124: Option<f64>,
    pub bhck3128: Option<f64>,
    pub bhck3153: Option<f64>,
    pub bhck3163: Option<f64>,
    pub bhck3164: Option<f64>,
    pub bhck3190: Option<f64>,
    pub bhck3197: Option<f64>,
    pub bhck3215: Option<f64>,
    pub bhck3216: Option<f64>,
    pub bhck3217: Option<f64>,
    pub bhck3230: Option<f64>,
    pub bhck3284: Option<f64>,
    pub bhck3296: Option<f64>,
    pub bhck3297: Option<f64>,
    pub bhck3298: Option<f64>,
    pub bhck3409: Option<f64>,
    pub bhck3411: Option<f64>,
    pub bhck3430: Option<f64>,
    pub bhck3434: Option<f64>,
    pub bhck3435: Option<f64>,
    pub bhck3450: Option<f64>,
    pub bhck3451: Option<bool>,
    pub bhck3452: Option<bool>,
    pub bhck3492: Option<f64>,
    pub bhck3493: Option<f64>,
    pub bhck3494: Option<f64>,
    pub bhck3495: Option<f64>,
    pub bhck3499: Option<f64>,
    pub bhck3500: Option<f64>,
    pub bhck3501: Option<f64>,
    pub bhck3502: Option<f64>,
    pub bhck3503: Option<f64>,
    pub bhck3504: Option<f64>,
    pub bhck3505: Option<f64>,
    pub bhck3506: Option<f64>,
    pub bhck3507: Option<f64>,
    pub bhck3508: Option<f64>,
    pub bhck3522: Option<bool>,
    pub bhck3528: Option<f64>,
    pub bhck3529: Option<f64>,
    pub bhck3530: Option<f64>,
    pub bhck3541: Option<f64>,
    pub bhck3546: Option<f64>,
    pub bhck3571: Option<f64>,
    pub bhck3572: Option<f64>,
    pub bhck3574: Option<f64>,
    pub bhck3576: Option<f64>,
    pub bhck3578: Option<f64>,
    pub bhck3580: Option<f64>,
    pub bhck3581: Option<f64>,
    pub bhck3582: Option<f64>,
    pub bhck3584: Option<f64>,
    pub bhck3588: Option<f64>,
    pub bhck3590: Option<f64>,
    pub bhck3656: Option<f64>,
    pub bhck3806: Option<f64>,
    pub bhck3809: Option<f64>,
    pub bhck3812: Option<f64>,
    pub bhck3816: Option<f64>,
    pub bhck3820: Option<f64>,
    pub bhck3822: Option<f64>,
    pub bhck3826: Option<f64>,
    pub bhck3836: Option<f64>,
    pub bhck3837: Option<f64>,
    pub bhck4010: Option<f64>,
    pub bhck4020: Option<f64>,
    pub bhck4027: Option<f64>,
    pub bhck4042: Option<f64>,
    pub bhck4059: Option<f64>,
    pub bhck4060: Option<f64>,
    pub bhck4065: Option<f64>,
    pub bhck4069: Option<f64>,
    pub bhck4070: Option<f64>,
    pub bhck4074: Option<f64>,
    pub bhck4078: Option<f64>,
    pub bhck4092: Option<f64>,
    pub bhck4105: Option<f64>,
    pub bhck4106: Option<f64>,
    pub bhck4115: Option<f64>,
    pub bhck4136: Option<f64>,
    pub bhck4141: Option<f64>,
    pub bhck4146: Option<f64>,
    pub bhck4150: Option<f64>,
    pub bhck4172: Option<f64>,
    pub bhck4180: Option<f64>,
    pub bhck4185: Option<f64>,
    pub bhck4217: Option<f64>,
    pub bhck4219: Option<f64>,
    pub bhck4300: Option<f64>,
    pub bhck4301: Option<f64>,
    pub bhck4302: Option<f64>,
    pub bhck4313: Option<f64>,
    pub bhck4320: Option<f64>,
    pub bhck4336: Option<f64>,
    pub bhck4340: Option<f64>,
    pub bhck4356: Option<f64>,
    pub bhck4393: Option<f64>,
    pub bhck4394: Option<f64>,
    pub bhck4395: Option<f64>,
    pub bhck4396: Option<f64>,
    pub bhck4397: Option<f64>,
    pub bhck4398: Option<f64>,
    pub bhck4399: Option<f64>,
    pub bhck4411: Option<f64>,
    pub bhck4412: Option<f64>,
    pub bhck4414: Option<f64>,
    pub bhck4435: Option<f64>,
    pub bhck4436: Option<f64>,
    pub bhck4460: Option<f64>,
    pub bhck4484: Option<f64>,
    pub bhck4503: Option<f64>,
    pub bhck4504: Option<f64>,
    pub bhck4506: Option<f64>,
    pub bhck4507: Option<f64>,
    pub bhck4518: Option<f64>,
    pub bhck4519: Option<f64>,
    pub bhck4531: Option<f64>,
    pub bhck4574: Option<f64>,
    pub bhck4591: Option<f64>,
    pub bhck4592: Option<f64>,
    pub bhck4598: Option<f64>,
    pub bhck4635: Option<f64>,
    pub bhck4643: Option<f64>,
    pub bhck4644: Option<f64>,
    pub bhck4645: Option<f64>,
    pub bhck4646: Option<f64>,
    pub bhck4651: Option<f64>,
    pub bhck4652: Option<f64>,
    pub bhck4653: Option<f64>,
    pub bhck4654: Option<f64>,
    pub bhck4655: Option<f64>,
    pub bhck4656: Option<f64>,
    pub bhck4657: Option<f64>,
    pub bhck4658: Option<f64>,
    pub bhck4659: Option<f64>,
    pub bhck4776: Option<f64>,
    pub bhck4815: Option<f64>,
    pub bhck4832: Option<f64>,
    pub bhck4833: Option<f64>,
    pub bhck4834: Option<f64>,
    pub bhck5041: Option<f64>,
    pub bhck5043: Option<f64>,
    pub bhck5045: Option<f64>,
    pub bhck5047: Option<f64>,
    pub bhck5310: Option<f64>,
    pub bhck5351: Option<f64>,
    pub bhck5354: Option<f64>,
    pub bhck5355: Option<f64>,
    pub bhck5356: Option<f64>,
    pub bhck5359: Option<f64>,
    pub bhck5360: Option<f64>,
    pub bhck5369: Option<f64>,
    pub bhck5377: Option<f64>,
    pub bhck5378: Option<f64>,
    pub bhck5379: Option<f64>,
    pub bhck5380: Option<f64>,
    pub bhck5381: Option<f64>,
    pub bhck5382: Option<f64>,
    pub bhck5383: Option<bool>,
    pub bhck5384: Option<f64>,
    pub bhck5385: Option<f64>,
    pub bhck5386: Option<bool>,
    pub bhck5387: Option<f64>,
    pub bhck5388: Option<f64>,
    pub bhck5389: Option<f64>,
    pub bhck5390: Option<f64>,
    pub bhck5391: Option<f64>,
    pub bhck5393: Option<f64>,
    pub bhck5397: Option<f64>,
    pub bhck5398: Option<f64>,
    pub bhck5399: Option<f64>,
    pub bhck5400: Option<f64>,
    pub bhck5401: Option<f64>,
    pub bhck5402: Option<f64>,
    pub bhck5403: Option<f64>,
    pub bhck5409: Option<f64>,
    pub bhck5411: Option<f64>,
    pub bhck5413: Option<f64>,
    pub bhck5459: Option<f64>,
    pub bhck5460: Option<f64>,
    pub bhck5461: Option<f64>,
    pub bhck5507: Option<f64>,
    pub bhck5610: Option<f64>,
    pub bhck5612: Option<f64>,
    pub bhck5613: Option<f64>,
    pub bhck5614: Option<f64>,
    pub bhck5615: Option<f64>,
    pub bhck5616: Option<f64>,
    pub bhck5617: Option<f64>,
    pub bhck6019: Option<f64>,
    pub bhck6373: Option<f64>,
    pub bhck6416: Option<f64>,
    pub bhck6438: Option<f64>,
    pub bhck6441: Option<f64>,
    pub bhck6442: Option<f64>,
    pub bhck6550: Option<f64>,
    pub bhck6555: Option<f64>,
    pub bhck6556: Option<f64>,
    pub bhck6557: Option<f64>,
    pub bhck6558: Option<f64>,
    pub bhck6559: Option<f64>,
    pub bhck6560: Option<f64>,
    pub bhck6561: Option<f64>,
    pub bhck6566: Option<f64>,
    pub bhck6572: Option<f64>,
    pub bhck6586: Option<f64>,
    pub bhck6599: Option<f64>,
    pub bhck6649: Option<f64>,
    pub bhck6669: Option<bool>,
    pub bhck6688: Option<f64>,
    pub bhck6689: Option<f64>,
    pub bhck6761: Option<f64>,
    pub bhck6765: Option<f64>,
    pub bhck6927: Option<bool>,
    pub bhck6928: Option<bool>,
    pub bhck6995: Option<bool>,
    pub bhck6998: Option<bool>,
    pub bhck8403: Option<f64>,
    pub bhck8427: Option<f64>,
    pub bhck8428: Option<f64>,
    pub bhck8429: Option<f64>,
    pub bhck8430: Option<f64>,
    pub bhck8431: Option<f64>,
    pub bhck8433: Option<f64>,
    pub bhck8434: Option<f64>,
    pub bhck8492: Option<f64>,
    pub bhck8493: Option<f64>,
    pub bhck8494: Option<f64>,
    pub bhck8495: Option<f64>,
    pub bhck8496: Option<f64>,
    pub bhck8497: Option<f64>,
    pub bhck8498: Option<f64>,
    pub bhck8499: Option<f64>,
    pub bhck8531: Option<f64>,
    pub bhck8532: Option<f64>,
    pub bhck8533: Option<f64>,
    pub bhck8534: Option<f64>,
    pub bhck8535: Option<f64>,
    pub bhck8536: Option<f64>,
    pub bhck8537: Option<f64>,
    pub bhck8538: Option<f64>,
    pub bhck8539: Option<f64>,
    pub bhck8540: Option<f64>,
    pub bhck8541: Option<f64>,
    pub bhck8542: Option<f64>,
    pub bhck8543: Option<f64>,
    pub bhck8544: Option<f64>,
    pub bhck8545: Option<f64>,
    pub bhck8546: Option<f64>,
    pub bhck8547: Option<f64>,
    pub bhck8548: Option<f64>,
    pub bhck8549: Option<f64>,
    pub bhck8550: Option<f64>,
    pub bhck8557: Option<f64>,
    pub bhck8558: Option<f64>,
    pub bhck8559: Option<f64>,
    pub bhck8560: Option<f64>,
    pub bhck8561: Option<f64>,
    pub bhck8562: Option<f64>,
    pub bhck8563: Option<f64>,
    pub bhck8564: Option<f64>,
    pub bhck8565: Option<f64>,
    pub bhck8566: Option<f64>,
    pub bhck8567: Option<f64>,
    pub bhck8693: Option<f64>,
    pub bhck8694: Option<f64>,
    pub bhck8695: Option<f64>,
    pub bhck8696: Option<f64>,
    pub bhck8697: Option<f64>,
    pub bhck8698: Option<f64>,
    pub bhck8699: Option<f64>,
    pub bhck8700: Option<f64>,
    pub bhck8719: Option<f64>,
    pub bhck8720: Option<f64>,
    pub bhck8733: Option<f64>,
    pub bhck8734: Option<f64>,
    pub bhck8735: Option<f64>,
    pub bhck8736: Option<f64>,
    pub bhck8737: Option<f64>,
    pub bhck8738: Option<f64>,
    pub bhck8739: Option<f64>,
    pub bhck8740: Option<f64>,
    pub bhck8741: Option<f64>,
    pub bhck8742: Option<f64>,
    pub bhck8743: Option<f64>,
    pub bhck8744: Option<f64>,
    pub bhck8745: Option<f64>,
    pub bhck8746: Option<f64>,
    pub bhck8747: Option<f64>,
    pub bhck8748: Option<f64>,
    pub bhck8749: Option<f64>,
    pub bhck8750: Option<f64>,
    pub bhck8751: Option<f64>,
    pub bhck8752: Option<f64>,
    pub bhck8753: Option<f64>,
    pub bhck8754: Option<f64>,
    pub bhck8755: Option<f64>,
    pub bhck8756: Option<f64>,
    pub bhck8757: Option<f64>,
    pub bhck8758: Option<f64>,
    pub bhck8759: Option<f64>,
    pub bhck8760: Option<f64>,
    pub bhck8761: Option<f64>,
    pub bhck8762: Option<f64>,
    pub bhck8763: Option<f64>,
    pub bhck8764: Option<f64>,
    pub bhck8766: Option<f64>,
    pub bhck8767: Option<f64>,
    pub bhck8769: Option<f64>,
    pub bhck8770: Option<f64>,
    pub bhck8771: Option<f64>,
    pub bhck8772: Option<f64>,
    pub bhck8773: Option<f64>,
    pub bhck8774: Option<f64>,
    pub bhck8775: Option<f64>,
    pub bhck8776: Option<f64>,
    pub bhck8777: Option<f64>,
    pub bhck8778: Option<f64>,
    pub bhck8779: Option<f64>,
    pub bhck8782: Option<f64>,
    pub bhck8783: Option<f64>,
    pub bhck8843: Option<f64>,
    pub bhcka000: Option<f64>,
    pub bhcka001: Option<f64>,
    pub bhcka002: Option<f64>,
    pub bhcka130: Option<f64>,
    pub bhcka221: Option<f64>,
    pub bhcka222: Option<f64>,
    pub bhcka224: Option<f64>,
    pub bhcka250: Option<f64>,
    pub bhcka251: Option<f64>,
    pub bhcka506: Option<f64>,
    pub bhcka507: Option<f64>,
    pub bhcka510: Option<f64>,
    pub bhcka511: Option<f64>,
    pub bhcka512: Option<f64>,
    pub bhcka517: Option<f64>,
    pub bhcka518: Option<f64>,
    pub bhcka519: Option<f64>,
    pub bhcka520: Option<f64>,
    pub bhcka521: Option<f64>,
    pub bhcka522: Option<f64>,
    pub bhcka523: Option<f64>,
    pub bhcka524: Option<f64>,
    pub bhcka525: Option<f64>,
    pub bhcka530: Option<f64>,
    pub bhcka534: Option<f64>,
    pub bhcka535: Option<f64>,
    pub bhckb026: Option<f64>,
    pub bhckb029: Option<f64>,
    pub bhckb030: Option<f64>,
    pub bhckb032: Option<f64>,
    pub bhckb035: Option<f64>,
    pub bhckb036: Option<f64>,
    pub bhckb039: Option<f64>,
    pub bhckb040: Option<f64>,
    pub bhckb044: Option<f64>,
    pub bhckb045: Option<f64>,
    pub bhckb047: Option<f64>,
    pub bhckb050: Option<f64>,
    pub bhckb051: Option<f64>,
    pub bhckb054: Option<f64>,
    pub bhckb055: Option<f64>,
    pub bhckb077: Option<f64>,
    pub bhckb488: Option<f64>,
    pub bhckb489: Option<f64>,
    pub bhckb490: Option<f64>,
    pub bhckb492: Option<f64>,
    pub bhckb493: Option<f64>,
    pub bhckb494: Option<f64>,
    pub bhckb496: Option<f64>,
    pub bhckb497: Option<f64>,
    pub bhckb500: Option<f64>,
    pub bhckb501: Option<f64>,
    pub bhckb502: Option<f64>,
    pub bhckb508: Option<f64>,
    pub bhckb511: Option<f64>,
    pub bhckb512: Option<f64>,
    pub bhckb514: Option<f64>,
    pub bhckb516: Option<f64>,
    pub bhckb522: Option<f64>,
    pub bhckb528: Option<f64>,
    pub bhckb529: Option<f64>,
    pub bhckb530: Option<f64>,
    pub bhckb538: Option<f64>,
    pub bhckb539: Option<f64>,
    pub bhckb546: Option<f64>,
    pub bhckb556: Option<f64>,
    pub bhckb557: Option<f64>,
    pub bhckb559: Option<f64>,
    pub bhckb560: Option<f64>,
    pub bhckb569: Option<f64>,
    pub bhckb570: Option<f64>,
    pub bhckb572: Option<f64>,
    pub bhckb573: Option<f64>,
    pub bhckb574: Option<f64>,
    pub bhckb575: Option<f64>,
    pub bhckb576: Option<f64>,
    pub bhckb577: Option<f64>,
    pub bhckb578: Option<f64>,
    pub bhckb579: Option<f64>,
    pub bhckb580: Option<f64>,
    pub bhckb588: Option<f64>,
    pub bhckb590: Option<f64>,
    pub bhckb591: Option<f64>,
    pub bhckb592: Option<f64>,
    pub bhckb593: Option<f64>,
    pub bhckb594: Option<f64>,
    pub bhckb595: Option<f64>,
    pub bhckb596: Option<f64>,
    pub bhckb639: Option<f64>,
    pub bhckb675: Option<f64>,
    pub bhckb681: Option<f64>,
    pub bhckb747: Option<f64>,
    pub bhckb748: Option<f64>,
    pub bhckb749: Option<f64>,
    pub bhckb750: Option<f64>,
    pub bhckb751: Option<f64>,
    pub bhckb752: Option<f64>,
    pub bhckb753: Option<f64>,
    pub bhckb761: Option<f64>,
    pub bhckb762: Option<f64>,
    pub bhckb763: Option<f64>,
    pub bhckb770: Option<f64>,
    pub bhckb771: Option<f64>,
    pub bhckb772: Option<f64>,
    pub bhckb776: Option<f64>,
    pub bhckb777: Option<f64>,
    pub bhckb778: Option<f64>,
    pub bhckb779: Option<f64>,
    pub bhckb780: Option<f64>,
    pub bhckb781: Option<f64>,
    pub bhckb782: Option<f64>,
    pub bhckb790: Option<f64>,
    pub bhckb791: Option<f64>,
    pub bhckb792: Option<f64>,
    pub bhckb793: Option<f64>,
    pub bhckb794: Option<f64>,
    pub bhckb795: Option<f64>,
    pub bhckb796: Option<f64>,
    pub bhckb797: Option<f64>,
    pub bhckb798: Option<f64>,
    pub bhckb799: Option<f64>,
    pub bhckb800: Option<f64>,
    pub bhckb801: Option<f64>,
    pub bhckb802: Option<f64>,
    pub bhckb803: Option<f64>,
    pub bhckb806: Option<f64>,
    pub bhckb807: Option<f64>,
    pub bhckb837: Option<f64>,
    pub bhckb838: Option<f64>,
    pub bhckb839: Option<f64>,
    pub bhckb840: Option<f64>,
    pub bhckb841: Option<f64>,
    pub bhckb842: Option<f64>,
    pub bhckb843: Option<f64>,
    pub bhckb844: Option<f64>,
    pub bhckb845: Option<f64>,
    pub bhckb846: Option<f64>,
    pub bhckb847: Option<f64>,
    pub bhckb848: Option<f64>,
    pub bhckb849: Option<f64>,
    pub bhckb850: Option<f64>,
    pub bhckb851: Option<f64>,
    pub bhckb852: Option<f64>,
    pub bhckb853: Option<f64>,
    pub bhckb854: Option<f64>,
    pub bhckb855: Option<f64>,
    pub bhckb856: Option<f64>,
    pub bhckb857: Option<f64>,
    pub bhckb858: Option<f64>,
    pub bhckb859: Option<f64>,
    pub bhckb860: Option<f64>,
    pub bhckb861: Option<f64>,
    pub bhckb983: Option<f64>,
    pub bhckb984: Option<f64>,
    pub bhckb985: Option<f64>,
    pub bhckb986: Option<bool>,
    pub bhckb988: Option<f64>,
    pub bhckb990: Option<f64>,
    pub bhckb991: Option<f64>,
    pub bhckb992: Option<f64>,
    pub bhckb994: Option<f64>,
    pub bhckb996: Option<f64>,
    pub bhckb998: Option<f64>,
    pub bhckc009: Option<f64>,
    pub bhckc013: Option<f64>,
    pub bhckc014: Option<f64>,
    pub bhckc016: Option<f64>,
    pub bhckc017: Option<f64>,
    pub bhckc050: Option<bool>,
    pub bhckc079: Option<f64>,
    pub bhckc159: Option<f64>,
    pub bhckc160: Option<f64>,
    pub bhckc161: Option<f64>,
    pub bhckc216: Option<f64>,
    pub bhckc219: Option<f64>,
    pub bhckc220: Option<f64>,
    pub bhckc221: Option<f64>,
    pub bhckc222: Option<f64>,
    pub bhckc225: Option<f64>,
    pub bhckc226: Option<f64>,
    pub bhckc229: Option<f64>,
    pub bhckc230: Option<f64>,
    pub bhckc231: Option<f64>,
    pub bhckc232: Option<f64>,
    pub bhckc233: Option<f64>,
    pub bhckc234: Option<f64>,
    pub bhckc235: Option<f64>,
    pub bhckc236: Option<f64>,
    pub bhckc237: Option<f64>,
    pub bhckc238: Option<f64>,
    pub bhckc239: Option<f64>,
    pub bhckc240: Option<f64>,
    pub bhckc241: Option<f64>,
    pub bhckc243: Option<f64>,
    pub bhckc246: Option<f64>,
    pub bhckc250: Option<f64>,
    pub bhckc251: Option<f64>,
    pub bhckc252: Option<f64>,
    pub bhckc253: Option<f64>,
    pub bhckc386: Option<f64>,
    pub bhckc387: Option<f64>,
    pub bhckc390: Option<f64>,
    pub bhckc410: Option<f64>,
    pub bhckc411: Option<f64>,
    pub bhckc435: Option<f64>,
    pub bhckc447: Option<f64>,
    pub bhckc498: Option<f64>,
    pub bhckc700: Option<f64>,
    pub bhckc701: Option<f64>,
    pub bhckc781: Option<f64>,
    pub bhckc880: Option<f64>,
    pub bhckc884: Option<f64>,
    pub bhckc886: Option<f64>,
    pub bhckc887: Option<f64>,
    pub bhckc888: Option<f64>,
    pub bhckc889: Option<f64>,
    pub bhckc890: Option<f64>,
    pub bhckc891: Option<f64>,
    pub bhckc892: Option<f64>,
    pub bhckc893: Option<f64>,
    pub bhckc894: Option<f64>,
    pub bhckc895: Option<f64>,
    pub bhckc896: Option<f64>,
    pub bhckc897: Option<f64>,
    pub bhckc898: Option<f64>,
    pub bhckc968: Option<f64>,
    pub bhckc969: Option<f64>,
    pub bhckc970: Option<f64>,
    pub bhckc971: Option<f64>,
    pub bhckc972: Option<f64>,
    pub bhckc973: Option<f64>,
    pub bhckc974: Option<f64>,
    pub bhckc975: Option<f64>,
    pub bhckc980: Option<f64>,
    pub bhckc981: Option<f64>,
    pub bhckc982: Option<f64>,
    pub bhckc983: Option<f64>,
    pub bhckc984: Option<f64>,
    pub bhckc985: Option<f64>,
    pub bhckc988: Option<f64>,
    pub bhckc989: Option<f64>,
    pub bhckd958: Option<f64>,
    pub bhckd959: Option<f64>,
    pub bhckd960: Option<f64>,
    pub bhckd962: Option<f64>,
    pub bhckd963: Option<f64>,
    pub bhckd964: Option<f64>,
    pub bhckd965: Option<f64>,
    pub bhckd967: Option<f64>,
    pub bhckd968: Option<f64>,
    pub bhckd969: Option<f64>,
    pub bhckd970: Option<f64>,
    pub bhckd971: Option<f64>,
    pub bhckd972: Option<f64>,
    pub bhckd973: Option<f64>,
    pub bhckd974: Option<f64>,
    pub bhckd982: Option<f64>,
    pub bhckd983: Option<f64>,
    pub bhckd984: Option<f64>,
    pub bhckd985: Option<f64>,
    pub bhckd991: Option<f64>,
    pub bhckd992: Option<f64>,
    pub bhckd993: Option<f64>,
    pub bhckd994: Option<f64>,
    pub bhckd995: Option<f64>,
    pub bhckd996: Option<f64>,
    pub bhckf031: Option<f64>,
    pub bhckf070: Option<f64>,
    pub bhckf071: Option<f64>,
    pub bhckf072: Option<f64>,
    pub bhckf073: Option<f64>,
    pub bhckf158: Option<f64>,
    pub bhckf159: Option<f64>,
    pub bhckf160: Option<f64>,
    pub bhckf161: Option<f64>,
    pub bhckf162: Option<f64>,
    pub bhckf163: Option<f64>,
    pub bhckf164: Option<f64>,
    pub bhckf165: Option<f64>,
    pub bhckf166: Option<f64>,
    pub bhckf167: Option<f64>,
    pub bhckf168: Option<f64>,
    pub bhckf169: Option<f64>,
    pub bhckf170: Option<f64>,
    pub bhckf171: Option<f64>,
    pub bhckf172: Option<f64>,
    pub bhckf173: Option<f64>,
    pub bhckf174: Option<f64>,
    pub bhckf175: Option<f64>,
    pub bhckf176: Option<f64>,
    pub bhckf177: Option<f64>,
    pub bhckf178: Option<f64>,
    pub bhckf179: Option<f64>,
    pub bhckf180: Option<f64>,
    pub bhckf181: Option<f64>,
    pub bhckf182: Option<f64>,
    pub bhckf183: Option<f64>,
    pub bhckf184: Option<f64>,
    pub bhckf185: Option<f64>,
    pub bhckf228: Option<f64>,
    pub bhckf229: Option<f64>,
    pub bhckf241: Option<f64>,
    pub bhckf242: Option<f64>,
    pub bhckf244: Option<f64>,
    pub bhckf245: Option<f64>,
    pub bhckf247: Option<f64>,
    pub bhckf248: Option<f64>,
    pub bhckf250: Option<f64>,
    pub bhckf251: Option<f64>,
    pub bhckf253: Option<f64>,
    pub bhckf254: Option<f64>,
    pub bhckf256: Option<f64>,
    pub bhckf257: Option<f64>,
    pub bhckf259: Option<f64>,
    pub bhckf260: Option<f64>,
    pub bhckf262: Option<f64>,
    pub bhckf263: Option<f64>,
    pub bhckf264: Option<f64>,
    pub bhckf465: Option<f64>,
    pub bhckf551: Option<f64>,
    pub bhckf552: Option<f64>,
    pub bhckf553: Option<f64>,
    pub bhckf554: Option<f64>,
    pub bhckf555: Option<f64>,
    pub bhckf556: Option<f64>,
    pub bhckf557: Option<f64>,
    pub bhckf558: Option<f64>,
    pub bhckf585: Option<f64>,
    pub bhckf586: Option<f64>,
    pub bhckf587: Option<f64>,
    pub bhckf588: Option<f64>,
    pub bhckf589: Option<f64>,
    pub bhckf608: Option<f64>,
    pub bhckf639: Option<f64>,
    pub bhckf640: Option<f64>,
    pub bhckf655: Option<f64>,
    pub bhckf658: Option<f64>,
    pub bhckf661: Option<f64>,
    pub bhckf662: Option<f64>,
    pub bhckf663: Option<f64>,
    pub bhckf664: Option<f64>,
    pub bhckf665: Option<f64>,
    pub bhckf666: Option<f64>,
    pub bhckf682: Option<f64>,
    pub bhckf683: Option<f64>,
    pub bhckf684: Option<f64>,
    pub bhckf685: Option<f64>,
    pub bhckf686: Option<f64>,
    pub bhckf687: Option<f64>,
    pub bhckf688: Option<f64>,
    pub bhckf689: Option<f64>,
    pub bhckf690: Option<f64>,
    pub bhckf691: Option<f64>,
    pub bhckf692: Option<f64>,
    pub bhckf693: Option<f64>,
    pub bhckf694: Option<f64>,
    pub bhckf695: Option<f64>,
    pub bhckf696: Option<f64>,
    pub bhckf697: Option<f64>,
    pub bhckf821: Option<f64>,
    pub bhckf841: Option<bool>,
    pub bhckft28: Option<f64>,
    pub bhckft29: Option<f64>,
    pub bhckft30: Option<f64>,
    pub bhckft31: Option<f64>,
    pub bhckft32: Option<f64>,
    pub bhckft41: Option<f64>,
    pub bhckft42: Option<bool>,
    pub bhckft43: Option<bool>,
    pub bhckft44: Option<bool>,
    pub bhckg091: Option<f64>,
    pub bhckg092: Option<f64>,
    pub bhckg093: Option<f64>,
    pub bhckg094: Option<f64>,
    pub bhckg095: Option<f64>,
    pub bhckg096: Option<f64>,
    pub bhckg097: Option<f64>,
    pub bhckg098: Option<f64>,
    pub bhckg099: Option<f64>,
    pub bhckg100: Option<f64>,
    pub bhckg101: Option<f64>,
    pub bhckg102: Option<f64>,
    pub bhckg103: Option<f64>,
    pub bhckg104: Option<f64>,
    pub bhckg209: Option<f64>,
    pub bhckg210: Option<f64>,
    pub bhckg211: Option<f64>,
    pub bhckg212: Option<f64>,
    pub bhckg213: Option<f64>,
    pub bhckg218: Option<f64>,
    pub bhckg221: Option<f64>,
    pub bhckg234: Option<f64>,
    pub bhckg235: Option<f64>,
    pub bhckg300: Option<f64>,
    pub bhckg301: Option<f64>,
    pub bhckg302: Option<f64>,
    pub bhckg303: Option<f64>,
    pub bhckg304: Option<f64>,
    pub bhckg305: Option<f64>,
    pub bhckg306: Option<f64>,
    pub bhckg307: Option<f64>,
    pub bhckg308: Option<f64>,
    pub bhckg309: Option<f64>,
    pub bhckg310: Option<f64>,
    pub bhckg311: Option<f64>,
    pub bhckg312: Option<f64>,
    pub bhckg313: Option<f64>,
    pub bhckg314: Option<f64>,
    pub bhckg315: Option<f64>,
    pub bhckg316: Option<f64>,
    pub bhckg317: Option<f64>,
    pub bhckg318: Option<f64>,
    pub bhckg319: Option<f64>,
    pub bhckg320: Option<f64>,
    pub bhckg321: Option<f64>,
    pub bhckg322: Option<f64>,
    pub bhckg323: Option<f64>,
    pub bhckg324: Option<f64>,
    pub bhckg325: Option<f64>,
    pub bhckg326: Option<f64>,
    pub bhckg327: Option<f64>,
    pub bhckg328: Option<f64>,
    pub bhckg329: Option<f64>,
    pub bhckg330: Option<f64>,
    pub bhckg331: Option<f64>,
    pub bhckg336: Option<f64>,
    pub bhckg337: Option<f64>,
    pub bhckg338: Option<f64>,
    pub bhckg339: Option<f64>,
    pub bhckg340: Option<f64>,
    pub bhckg341: Option<f64>,
    pub bhckg342: Option<f64>,
    pub bhckg343: Option<f64>,
    pub bhckg344: Option<f64>,
    pub bhckg345: Option<f64>,
    pub bhckg346: Option<f64>,
    pub bhckg347: Option<f64>,
    pub bhckg391: Option<f64>,
    pub bhckg392: Option<f64>,
    pub bhckg395: Option<f64>,
    pub bhckg396: Option<f64>,
    pub bhckg401: Option<f64>,
    pub bhckg402: Option<f64>,
    pub bhckg403: Option<f64>,
    pub bhckg404: Option<f64>,
    pub bhckg405: Option<f64>,
    pub bhckg406: Option<f64>,
    pub bhckg407: Option<f64>,
    pub bhckg408: Option<f64>,
    pub bhckg409: Option<f64>,
    pub bhckg410: Option<f64>,
    pub bhckg411: Option<f64>,
    pub bhckg412: Option<f64>,
    pub bhckg413: Option<f64>,
    pub bhckg414: Option<f64>,
    pub bhckg415: Option<f64>,
    pub bhckg416: Option<f64>,
    pub bhckg417: Option<f64>,
    pub bhckg474: Option<f64>,
    pub bhckg475: Option<f64>,
    pub bhckg476: Option<f64>,
    pub bhckg477: Option<f64>,
    pub bhckg478: Option<f64>,
    pub bhckg479: Option<f64>,
    pub bhckg480: Option<f64>,
    pub bhckg481: Option<f64>,
    pub bhckg482: Option<f64>,
    pub bhckg483: Option<f64>,
    pub bhckg484: Option<f64>,
    pub bhckg485: Option<f64>,
    pub bhckg486: Option<f64>,
    pub bhckg487: Option<f64>,
    pub bhckg488: Option<f64>,
    pub bhckg489: Option<f64>,
    pub bhckg490: Option<f64>,
    pub bhckg491: Option<f64>,
    pub bhckg492: Option<f64>,
    pub bhckg507: Option<f64>,
    pub bhckg508: Option<f64>,
    pub bhckg509: Option<f64>,
    pub bhckg510: Option<f64>,
    pub bhckg511: Option<f64>,
    pub bhckg521: Option<f64>,
    pub bhckg522: Option<f64>,
    pub bhckg523: Option<f64>,
    pub bhckg524: Option<f64>,
    pub bhckg525: Option<f64>,
    pub bhckg536: Option<f64>,
    pub bhckg537: Option<f64>,
    pub bhckg538: Option<f64>,
    pub bhckg539: Option<f64>,
    pub bhckg540: Option<f64>,
    pub bhckg541: Option<f64>,
    pub bhckg542: Option<f64>,
    pub bhckg543: Option<f64>,
    pub bhckg544: Option<f64>,
    pub bhckg545: Option<f64>,
    pub bhckg546: Option<f64>,
    pub bhckg547: Option<f64>,
    pub bhckg548: Option<f64>,
    pub bhckg549: Option<f64>,
    pub bhckg550: Option<f64>,
    pub bhckg561: Option<f64>,
    pub bhckg562: Option<f64>,
    pub bhckg563: Option<f64>,
    pub bhckg564: Option<f64>,
    pub bhckg565: Option<f64>,
    pub bhckg566: Option<f64>,
    pub bhckg567: Option<f64>,
    pub bhckg568: Option<f64>,
    pub bhckg569: Option<f64>,
    pub bhckg570: Option<f64>,
    pub bhckg571: Option<f64>,
    pub bhckg572: Option<f64>,
    pub bhckg573: Option<f64>,
    pub bhckg574: Option<f64>,
    pub bhckg575: Option<f64>,
    pub bhckg586: Option<f64>,
    pub bhckg587: Option<f64>,
    pub bhckg588: Option<f64>,
    pub bhckg589: Option<f64>,
    pub bhckg590: Option<f64>,
    pub bhckg597: Option<f64>,
    pub bhckg598: Option<f64>,
    pub bhckg599: Option<f64>,
    pub bhckg600: Option<f64>,
    pub bhckg601: Option<f64>,
    pub bhckg602: Option<f64>,
    pub bhckg606: Option<f64>,
    pub bhckg607: Option<f64>,
    pub bhckg608: Option<f64>,
    pub bhckg609: Option<f64>,
    pub bhckg610: Option<f64>,
    pub bhckg611: Option<f64>,
    pub bhckg618: Option<f64>,
    pub bhckg619: Option<f64>,
    pub bhckg620: Option<f64>,
    pub bhckg621: Option<f64>,
    pub bhckg622: Option<f64>,
    pub bhckg623: Option<f64>,
    pub bhckg642: Option<f64>,
    pub bhckg804: Option<f64>,
    pub bhckg805: Option<f64>,
    pub bhckg806: Option<f64>,
    pub bhckg807: Option<f64>,
    pub bhckg808: Option<f64>,
    pub bhckg809: Option<f64>,
    pub bhckg894: Option<f64>,
    pub bhckg914: Option<f64>,
    pub bhckh172: Option<f64>,
    pub bhckh173: Option<f64>,
    pub bhckh174: Option<f64>,
    pub bhckh175: Option<f64>,
    pub bhckh176: Option<f64>,
    pub bhckh177: Option<f64>,
    pub bhckh178: Option<f64>,
    pub bhckh179: Option<f64>,
    pub bhckh180: Option<f64>,
    pub bhckh181: Option<f64>,
    pub bhckh182: Option<f64>,
    pub bhckh185: Option<f64>,
    pub bhckh186: Option<f64>,
    pub bhckh187: Option<f64>,
    pub bhckh188: Option<f64>,
    pub bhckh193: Option<f64>,
    pub bhckh194: Option<f64>,
    pub bhckh195: Option<f64>,
    pub bhckh196: Option<f64>,
    pub bhckh197: Option<f64>,
    pub bhckh198: Option<f64>,
    pub bhckh199: Option<f64>,
    pub bhckh200: Option<f64>,
    pub bhckh270: Option<f64>,
    pub bhckh271: Option<f64>,
    pub bhckh272: Option<f64>,
    pub bhckh273: Option<f64>,
    pub bhckh274: Option<f64>,
    pub bhckh275: Option<f64>,
    pub bhckh276: Option<f64>,
    pub bhckh277: Option<f64>,
    pub bhckh278: Option<f64>,
    pub bhckh279: Option<f64>,
    pub bhckh280: Option<f64>,
    pub bhckh281: Option<f64>,
    pub bhckh282: Option<f64>,
    pub bhckh283: Option<f64>,
    pub bhckh284: Option<f64>,
    pub bhckh285: Option<f64>,
    pub bhckh286: Option<f64>,
    pub bhckh287: Option<f64>,
    pub bhckh288: Option<f64>,
    pub bhckh293: Option<f64>,
    pub bhckh294: Option<f64>,
    pub bhckh295: Option<f64>,
    pub bhckh296: Option<f64>,
    pub bhckh297: Option<f64>,
    pub bhckh298: Option<f64>,
    pub bhckh299: Option<f64>,
    pub bhckhj78: Option<f64>,
    pub bhckhj79: Option<f64>,
    pub bhckhj80: Option<f64>,
    pub bhckhj81: Option<f64>,
    pub bhckhj82: Option<f64>,
    pub bhckhj83: Option<f64>,
    pub bhckhj84: Option<f64>,
    pub bhckhj85: Option<f64>,
    pub bhckhj88: Option<f64>,
    pub bhckhj89: Option<f64>,
    pub bhckhj92: Option<f64>,
    pub bhckhj93: Option<f64>,
    pub bhckhj94: Option<f64>,
    pub bhckhj95: Option<f64>,
    pub bhckhk03: Option<f64>,
    pub bhckhk04: Option<f64>,
    pub bhckht58: Option<f64>,
    pub bhckht59: Option<f64>,
    pub bhckht60: Option<f64>,
    pub bhckht61: Option<f64>,
    pub bhckht62: Option<f64>,
    pub bhckht63: Option<f64>,
    pub bhckht64: Option<f64>,
    pub bhckht65: Option<f64>,
    pub bhckht69: Option<f64>,
    pub bhckht80: Option<f64>,
    pub bhckht83: Option<f64>,
    pub bhckht84: Option<f64>,
    pub bhckht85: Option<f64>,
    pub bhckht87: Option<f64>,
    pub bhckht88: Option<f64>,
    pub bhckht89: Option<f64>,
    pub bhckht91: Option<f64>,
    pub bhckht92: Option<f64>,
    pub bhckht93: Option<f64>,
    pub bhckhu09: Option<f64>,
    pub bhckhu10: Option<f64>,
    pub bhckhu11: Option<f64>,
    pub bhckhu12: Option<f64>,
    pub bhckhu13: Option<f64>,
    pub bhckhu14: Option<f64>,
    pub bhckhu15: Option<f64>,
    pub bhckhu20: Option<f64>,
    pub bhckhu21: Option<f64>,
    pub bhckhu22: Option<f64>,
    pub bhckhu23: Option<f64>,
    pub bhckj320: Option<f64>,
    pub bhckj447: Option<f64>,
    pub bhckj451: Option<f64>,
    pub bhckj452: Option<f64>,
    pub bhckj453: Option<f64>,
    pub bhckj454: Option<f64>,
    pub bhckj455: Option<f64>,
    pub bhckj456: Option<f64>,
    pub bhckj461: Option<f64>,
    pub bhckj462: Option<f64>,
    pub bhckj463: Option<f64>,
    pub bhckj536: Option<f64>,
    pub bhckj537: Option<f64>,
    pub bhckj981: Option<f64>,
    pub bhckj982: Option<f64>,
    pub bhckj983: Option<f64>,
    pub bhckj984: Option<f64>,
    pub bhckj985: Option<f64>,
    pub bhckj986: Option<f64>,
    pub bhckj987: Option<f64>,
    pub bhckj988: Option<f64>,
    pub bhckj989: Option<f64>,
    pub bhckj990: Option<f64>,
    pub bhckj991: Option<f64>,
    pub bhckj992: Option<f64>,
    pub bhckj993: Option<f64>,
    pub bhckj994: Option<f64>,
    pub bhckj995: Option<f64>,
    pub bhckj996: Option<f64>,
    pub bhckj997: Option<f64>,
    pub bhckj998: Option<f64>,
    pub bhckj999: Option<f64>,
    pub bhckja21: Option<f64>,
    pub bhckja22: Option<f64>,
    pub bhckjf76: Option<f64>,
    pub bhckjf84: Option<f64>,
    pub bhckjf85: Option<f64>,
    pub bhckjf86: Option<f64>,
    pub bhckjf87: Option<f64>,
    pub bhckjf88: Option<f64>,
    pub bhckjf89: Option<f64>,
    pub bhckjf90: Option<f64>,
    pub bhckjf91: Option<f64>,
    pub bhckjf92: Option<f64>,
    pub bhckjf93: Option<f64>,
    pub bhckjh88: Option<f64>,
    pub bhckjh91: Option<f64>,
    pub bhckjh92: Option<f64>,
    pub bhckjh93: Option<f64>,
    pub bhckjh94: Option<f64>,
    pub bhckjh97: Option<f64>,
    pub bhckjh98: Option<f64>,
    pub bhckjh99: Option<f64>,
    pub bhckjj00: Option<f64>,
    pub bhckjj01: Option<f64>,
    pub bhckjj03: Option<f64>,
    pub bhckjj04: Option<f64>,
    pub bhckjj05: Option<f64>,
    pub bhckjj06: Option<f64>,
    pub bhckjj07: Option<f64>,
    pub bhckjj08: Option<f64>,
    pub bhckjj09: Option<f64>,
    pub bhckjj11: Option<f64>,
    pub bhckjj12: Option<f64>,
    pub bhckjj13: Option<f64>,
    pub bhckjj14: Option<f64>,
    pub bhckjj15: Option<f64>,
    pub bhckjj16: Option<f64>,
    pub bhckjj17: Option<f64>,
    pub bhckjj18: Option<f64>,
    pub bhckjj19: Option<f64>,
    pub bhckjj20: Option<f64>,
    pub bhckjj21: Option<f64>,
    pub bhckjj23: Option<f64>,
    pub bhckjj24: Option<f64>,
    pub bhckjj25: Option<f64>,
    pub bhckjj26: Option<f64>,
    pub bhckjj27: Option<f64>,
    pub bhckjj28: Option<f64>,
    pub bhckjj30: Option<f64>,
    pub bhckjj31: Option<f64>,
    pub bhckjj32: Option<f64>,
    pub bhckjj34: Option<f64>,
    pub bhckk001: Option<f64>,
    pub bhckk002: Option<f64>,
    pub bhckk003: Option<f64>,
    pub bhckk004: Option<f64>,
    pub bhckk005: Option<f64>,
    pub bhckk006: Option<f64>,
    pub bhckk007: Option<f64>,
    pub bhckk008: Option<f64>,
    pub bhckk009: Option<f64>,
    pub bhckk010: Option<f64>,
    pub bhckk011: Option<f64>,
    pub bhckk012: Option<f64>,
    pub bhckk013: Option<f64>,
    pub bhckk014: Option<f64>,
    pub bhckk015: Option<f64>,
    pub bhckk016: Option<f64>,
    pub bhckk017: Option<f64>,
    pub bhckk018: Option<f64>,
    pub bhckk019: Option<f64>,
    pub bhckk020: Option<f64>,
    pub bhckk021: Option<f64>,
    pub bhckk022: Option<f64>,
    pub bhckk023: Option<f64>,
    pub bhckk024: Option<f64>,
    pub bhckk025: Option<f64>,
    pub bhckk026: Option<f64>,
    pub bhckk027: Option<f64>,
    pub bhckk028: Option<f64>,
    pub bhckk029: Option<f64>,
    pub bhckk030: Option<f64>,
    pub bhckk031: Option<f64>,
    pub bhckk032: Option<f64>,
    pub bhckk033: Option<f64>,
    pub bhckk034: Option<f64>,
    pub bhckk035: Option<f64>,
    pub bhckk036: Option<f64>,
    pub bhckk037: Option<f64>,
    pub bhckk038: Option<f64>,
    pub bhckk039: Option<f64>,
    pub bhckk040: Option<f64>,
    pub bhckk041: Option<f64>,
    pub bhckk072: Option<f64>,
    pub bhckk073: Option<f64>,
    pub bhckk074: Option<f64>,
    pub bhckk075: Option<f64>,
    pub bhckk076: Option<f64>,
    pub bhckk077: Option<f64>,
    pub bhckk078: Option<f64>,
    pub bhckk079: Option<f64>,
    pub bhckk080: Option<f64>,
    pub bhckk081: Option<f64>,
    pub bhckk082: Option<f64>,
    pub bhckk083: Option<f64>,
    pub bhckk084: Option<f64>,
    pub bhckk085: Option<f64>,
    pub bhckk086: Option<f64>,
    pub bhckk087: Option<f64>,
    pub bhckk088: Option<f64>,
    pub bhckk089: Option<f64>,
    pub bhckk090: Option<f64>,
    pub bhckk091: Option<f64>,
    pub bhckk092: Option<f64>,
    pub bhckk093: Option<f64>,
    pub bhckk094: Option<f64>,
    pub bhckk095: Option<f64>,
    pub bhckk096: Option<f64>,
    pub bhckk097: Option<f64>,
    pub bhckk098: Option<f64>,
    pub bhckk099: Option<f64>,
    pub bhckk100: Option<f64>,
    pub bhckk101: Option<f64>,
    pub bhckk120: Option<f64>,
    pub bhckk121: Option<f64>,
    pub bhckk122: Option<f64>,
    pub bhckk123: Option<f64>,
    pub bhckk124: Option<f64>,
    pub bhckk125: Option<f64>,
    pub bhckk126: Option<f64>,
    pub bhckk127: Option<f64>,
    pub bhckk128: Option<f64>,
    pub bhckk129: Option<f64>,
    pub bhckk134: Option<f64>,
    pub bhckk135: Option<f64>,
    pub bhckk136: Option<f64>,
    pub bhckk137: Option<f64>,
    pub bhckk138: Option<f64>,
    pub bhckk139: Option<f64>,
    pub bhckk140: Option<f64>,
    pub bhckk142: Option<f64>,
    pub bhckk143: Option<f64>,
    pub bhckk144: Option<f64>,
    pub bhckk145: Option<f64>,
    pub bhckk146: Option<f64>,
    pub bhckk147: Option<f64>,
    pub bhckk148: Option<f64>,
    pub bhckk149: Option<f64>,
    pub bhckk150: Option<f64>,
    pub bhckk151: Option<f64>,
    pub bhckk152: Option<f64>,
    pub bhckk153: Option<f64>,
    pub bhckk154: Option<f64>,
    pub bhckk155: Option<f64>,
    pub bhckk156: Option<f64>,
    pub bhckk157: Option<f64>,
    pub bhckk163: Option<f64>,
    pub bhckk164: Option<f64>,
    pub bhckk165: Option<f64>,
    pub bhckk167: Option<f64>,
    pub bhckk168: Option<f64>,
    pub bhckk178: Option<f64>,
    pub bhckk179: Option<f64>,
    pub bhckk180: Option<f64>,
    pub bhckk181: Option<f64>,
    pub bhckk182: Option<f64>,
    pub bhckk183: Option<f64>,
    pub bhckk184: Option<f64>,
    pub bhckk185: Option<f64>,
    pub bhckk186: Option<f64>,
    pub bhckk192: Option<f64>,
    pub bhckk193: Option<f64>,
    pub bhckk194: Option<f64>,
    pub bhckk196: Option<f64>,
    pub bhckk201: Option<f64>,
    pub bhckk202: Option<f64>,
    pub bhckk203: Option<f64>,
    pub bhckk204: Option<f64>,
    pub bhckk205: Option<f64>,
    pub bhckk207: Option<f64>,
    pub bhckk208: Option<f64>,
    pub bhckk212: Option<f64>,
    pub bhckk213: Option<f64>,
    pub bhckk214: Option<f64>,
    pub bhckk215: Option<f64>,
    pub bhckk216: Option<f64>,
    pub bhckk217: Option<f64>,
    pub bhckk218: Option<f64>,
    pub bhckk267: Option<f64>,
    pub bhckk269: Option<f64>,
    pub bhckk270: Option<f64>,
    pub bhckk271: Option<f64>,
    pub bhckk272: Option<f64>,
    pub bhckk273: Option<f64>,
    pub bhckk274: Option<f64>,
    pub bhckk275: Option<f64>,
    pub bhckk276: Option<f64>,
    pub bhckk277: Option<f64>,
    pub bhckk278: Option<f64>,
    pub bhckk279: Option<f64>,
    pub bhckk280: Option<f64>,
    pub bhckk281: Option<f64>,
    pub bhckk282: Option<f64>,
    pub bhckk283: Option<f64>,
    pub bhckk284: Option<f64>,
    pub bhckk285: Option<f64>,
    pub bhckk286: Option<f64>,
    pub bhckk287: Option<f64>,
    pub bhckk288: Option<f64>,
    pub bhckkx46: Option<f64>,
    pub bhckkx47: Option<f64>,
    pub bhckkx50: Option<f64>,
    pub bhckkx51: Option<f64>,
    pub bhckkx52: Option<f64>,
    pub bhckkx53: Option<f64>,
    pub bhckkx54: Option<f64>,
    pub bhckkx55: Option<f64>,
    pub bhckkx57: Option<f64>,
    pub bhckkx58: Option<f64>,
    pub bhckkx60: Option<f64>,
    pub bhckkx61: Option<f64>,
    pub bhckkx62: Option<f64>,
    pub bhckkx63: Option<f64>,
    pub bhckkx64: Option<f64>,
    pub bhckkx65: Option<f64>,
    pub bhckky38: Option<bool>,
    pub bhcklg24: Option<bool>,
    pub bhcklg26: Option<f64>,
    pub bhckm727: Option<f64>,
    pub bhckm728: Option<f64>,
    pub bhckm729: Option<f64>,
    pub bhckm730: Option<f64>,
    pub bhckm731: Option<f64>,
    pub bhckm732: Option<f64>,
    pub bhckm733: Option<f64>,
    pub bhckm734: Option<f64>,
    pub bhckm735: Option<f64>,
    pub bhckm736: Option<f64>,
    pub bhckm737: Option<f64>,
    pub bhckm738: Option<f64>,
    pub bhckm739: Option<f64>,
    pub bhckm740: Option<f64>,
    pub bhckm741: Option<f64>,
    pub bhckm742: Option<f64>,
    pub bhckm743: Option<f64>,
    pub bhckm744: Option<f64>,
    pub bhckm962: Option<f64>,
    pub bhckmg94: Option<f64>,
    pub bhcks396: Option<f64>,
    pub bhcks397: Option<f64>,
    pub bhcks398: Option<f64>,
    pub bhcks399: Option<f64>,
    pub bhcks400: Option<f64>,
    pub bhcks402: Option<f64>,
    pub bhcks403: Option<f64>,
    pub bhcks405: Option<f64>,
    pub bhcks406: Option<f64>,
    pub bhcks410: Option<f64>,
    pub bhcks411: Option<f64>,
    pub bhcks414: Option<f64>,
    pub bhcks415: Option<f64>,
    pub bhcks416: Option<f64>,
    pub bhcks417: Option<f64>,
    pub bhcks420: Option<f64>,
    pub bhcks421: Option<f64>,
    pub bhcks424: Option<f64>,
    pub bhcks425: Option<f64>,
    pub bhcks426: Option<f64>,
    pub bhcks427: Option<f64>,
    pub bhcks428: Option<f64>,
    pub bhcks429: Option<f64>,
    pub bhcks432: Option<f64>,
    pub bhcks433: Option<f64>,
    pub bhcks434: Option<f64>,
    pub bhcks435: Option<f64>,
    pub bhcks436: Option<f64>,
    pub bhcks437: Option<f64>,
    pub bhcks440: Option<f64>,
    pub bhcks441: Option<f64>,
    pub bhcks442: Option<f64>,
    pub bhcks443: Option<f64>,
    pub bhcks446: Option<f64>,
    pub bhcks447: Option<f64>,
    pub bhcks450: Option<f64>,
    pub bhcks451: Option<f64>,
    pub bhcks452: Option<f64>,
    pub bhcks453: Option<f64>,
    pub bhcks454: Option<f64>,
    pub bhcks455: Option<f64>,
    pub bhcks458: Option<f64>,
    pub bhcks459: Option<f64>,
    pub bhcks460: Option<f64>,
    pub bhcks461: Option<f64>,
    pub bhcks462: Option<f64>,
    pub bhcks463: Option<f64>,
    pub bhcks469: Option<f64>,
    pub bhcks470: Option<f64>,
    pub bhcks471: Option<f64>,
    pub bhcks476: Option<f64>,
    pub bhcks477: Option<f64>,
    pub bhcks478: Option<f64>,
    pub bhcks479: Option<f64>,
    pub bhcks481: Option<f64>,
    pub bhcks482: Option<f64>,
    pub bhcks483: Option<f64>,
    pub bhcks484: Option<f64>,
    pub bhcks486: Option<f64>,
    pub bhcks487: Option<f64>,
    pub bhcks488: Option<f64>,
    pub bhcks489: Option<f64>,
    pub bhcks491: Option<f64>,
    pub bhcks492: Option<f64>,
    pub bhcks493: Option<f64>,
    pub bhcks494: Option<f64>,
    pub bhcks496: Option<f64>,
    pub bhcks497: Option<f64>,
    pub bhcks498: Option<f64>,
    pub bhcks499: Option<f64>,
    pub bhcks511: Option<f64>,
    pub bhcks513: Option<f64>,
    pub bhcks524: Option<f64>,
    pub bhcks549: Option<f64>,
    pub bhcks550: Option<f64>,
    pub bhcks551: Option<f64>,
    pub bhcks552: Option<f64>,
    pub bhcks554: Option<f64>,
    pub bhcks555: Option<f64>,
    pub bhcks556: Option<f64>,
    pub bhcks557: Option<f64>,
    pub bhcks582: Option<f64>,
    pub bhcks583: Option<f64>,
    pub bhcks584: Option<f64>,
    pub bhcks585: Option<f64>,
    pub bhcks586: Option<f64>,
    pub bhcks587: Option<f64>,
    pub bhcks588: Option<f64>,
    pub bhcks589: Option<f64>,
    pub bhcks590: Option<f64>,
    pub bhcks591: Option<f64>,
    pub bhcks592: Option<f64>,
    pub bhcks593: Option<f64>,
    pub bhcks594: Option<f64>,
    pub bhcks595: Option<f64>,
    pub bhcks596: Option<f64>,
    pub bhcks597: Option<f64>,
    pub bhcks598: Option<f64>,
    pub bhcks599: Option<f64>,
    pub bhcks600: Option<f64>,
    pub bhcks601: Option<f64>,
    pub bhcks602: Option<f64>,
    pub bhcks603: Option<f64>,
    pub bhcks604: Option<f64>,
    pub bhcks605: Option<f64>,
    pub bhcks606: Option<f64>,
    pub bhcks607: Option<f64>,
    pub bhcks608: Option<f64>,
    pub bhcks609: Option<f64>,
    pub bhcks610: Option<f64>,
    pub bhcks611: Option<f64>,
    pub bhcks612: Option<f64>,
    pub bhcks613: Option<f64>,
    pub bhcks614: Option<f64>,
    pub bhcks615: Option<f64>,
    pub bhcks616: Option<f64>,
    pub bhcks617: Option<f64>,
    pub bhcks618: Option<f64>,
    pub bhcks619: Option<f64>,
    pub bhcks620: Option<f64>,
    pub bhcks621: Option<f64>,
    pub bhcks622: Option<f64>,
    pub bhcks623: Option<f64>,
    pub bhckt047: Option<f64>,
    pub bhcky923: Option<f64>,
    pub bhcky924: Option<f64>,
    pub rssd9001: Option<f64>, // Option<i64>,
    pub rssd9017: Option<String>,
    pub rssd9999: Option<f64>, // Option<NaiveDate>,
    pub wrdsdownloaddate: Option<NaiveDate>,
}

impl SurrealCrudModel for BhckSeries1 {
    fn table() -> &'static str {
        "bhck_series1"
    }
    fn id_key(&self) -> Option<String> {
        match (self.rssd9001, self.rssd9999) {
            (Some(rssd9001), Some(rssd9999)) => Some(format!("{rssd9001}:{rssd9999}")),
            _ => None,
        }
    }
}

impl DuckCrudModel for BhckSeries1 {
    fn table() -> &'static str {
        "bhck_series1"
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl ToPolars for BhckSeries1 {
    fn schema() -> Schema {
        BhckSeries1::polars_schema()
    }
}

impl BhckSeries1 {
    pub fn polars_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("bhck0010".into(), DataType::Float64),
            Field::new("bhck0081".into(), DataType::Float64),
            Field::new("bhck0211".into(), DataType::Float64),
            Field::new("bhck0213".into(), DataType::Float64),
            Field::new("bhck0379".into(), DataType::Float64),
            Field::new("bhck0395".into(), DataType::Float64),
            Field::new("bhck0397".into(), DataType::Float64),
            Field::new("bhck0426".into(), DataType::Float64),
            Field::new("bhck0497".into(), DataType::Float64),
            Field::new("bhck1226".into(), DataType::Float64),
            Field::new("bhck1227".into(), DataType::Float64),
            Field::new("bhck1228".into(), DataType::Float64),
            Field::new("bhck1286".into(), DataType::Float64),
            Field::new("bhck1287".into(), DataType::Float64),
            Field::new("bhck1288".into(), DataType::Float64),
            Field::new("bhck1289".into(), DataType::Float64),
            Field::new("bhck1290".into(), DataType::Float64),
            Field::new("bhck1291".into(), DataType::Float64),
            Field::new("bhck1292".into(), DataType::Float64),
            Field::new("bhck1293".into(), DataType::Float64),
            Field::new("bhck1294".into(), DataType::Float64),
            Field::new("bhck1295".into(), DataType::Float64),
            Field::new("bhck1296".into(), DataType::Float64),
            Field::new("bhck1297".into(), DataType::Float64),
            Field::new("bhck1298".into(), DataType::Float64),
            Field::new("bhck1350".into(), DataType::Float64),
            Field::new("bhck1410".into(), DataType::Float64),
            Field::new("bhck1421".into(), DataType::Boolean),
            Field::new("bhck1422".into(), DataType::Float64),
            Field::new("bhck1423".into(), DataType::Float64),
            Field::new("bhck1545".into(), DataType::Float64),
            Field::new("bhck1563".into(), DataType::Float64),
            Field::new("bhck1564".into(), DataType::Float64),
            Field::new("bhck1583".into(), DataType::Float64),
            Field::new("bhck1590".into(), DataType::Float64),
            Field::new("bhck1594".into(), DataType::Float64),
            Field::new("bhck1597".into(), DataType::Float64),
            Field::new("bhck1606".into(), DataType::Float64),
            Field::new("bhck1607".into(), DataType::Float64),
            Field::new("bhck1608".into(), DataType::Float64),
            Field::new("bhck1611".into(), DataType::Float64),
            Field::new("bhck1612".into(), DataType::Float64),
            Field::new("bhck1613".into(), DataType::Float64),
            Field::new("bhck1615".into(), DataType::Float64),
            Field::new("bhck1616".into(), DataType::Float64),
            Field::new("bhck1635".into(), DataType::Float64),
            Field::new("bhck1636".into(), DataType::Float64),
            Field::new("bhck1638".into(), DataType::Float64),
            Field::new("bhck1639".into(), DataType::Float64),
            Field::new("bhck1651".into(), DataType::Float64),
            Field::new("bhck1698".into(), DataType::Float64),
            Field::new("bhck1699".into(), DataType::Float64),
            Field::new("bhck1701".into(), DataType::Float64),
            Field::new("bhck1702".into(), DataType::Float64),
            Field::new("bhck1703".into(), DataType::Float64),
            Field::new("bhck1705".into(), DataType::Float64),
            Field::new("bhck1706".into(), DataType::Float64),
            Field::new("bhck1707".into(), DataType::Float64),
            Field::new("bhck1709".into(), DataType::Float64),
            Field::new("bhck1710".into(), DataType::Float64),
            Field::new("bhck1711".into(), DataType::Float64),
            Field::new("bhck1713".into(), DataType::Float64),
            Field::new("bhck1714".into(), DataType::Float64),
            Field::new("bhck1715".into(), DataType::Float64),
            Field::new("bhck1716".into(), DataType::Float64),
            Field::new("bhck1717".into(), DataType::Float64),
            Field::new("bhck1718".into(), DataType::Float64),
            Field::new("bhck1719".into(), DataType::Float64),
            Field::new("bhck1727".into(), DataType::Float64),
            Field::new("bhck1731".into(), DataType::Float64),
            Field::new("bhck1732".into(), DataType::Float64),
            Field::new("bhck1733".into(), DataType::Float64),
            Field::new("bhck1734".into(), DataType::Float64),
            Field::new("bhck1735".into(), DataType::Float64),
            Field::new("bhck1736".into(), DataType::Float64),
            Field::new("bhck1737".into(), DataType::Float64),
            Field::new("bhck1738".into(), DataType::Float64),
            Field::new("bhck1739".into(), DataType::Float64),
            Field::new("bhck1741".into(), DataType::Float64),
            Field::new("bhck1742".into(), DataType::Float64),
            Field::new("bhck1743".into(), DataType::Float64),
            Field::new("bhck1744".into(), DataType::Float64),
            Field::new("bhck1746".into(), DataType::Float64),
            Field::new("bhck1752".into(), DataType::Float64),
            Field::new("bhck1753".into(), DataType::Float64),
            Field::new("bhck1754".into(), DataType::Float64),
            Field::new("bhck1755".into(), DataType::Float64),
            Field::new("bhck1763".into(), DataType::Float64),
            Field::new("bhck1764".into(), DataType::Float64),
            Field::new("bhck1766".into(), DataType::Float64),
            Field::new("bhck1773".into(), DataType::Float64),
            Field::new("bhck1778".into(), DataType::Float64),
            Field::new("bhck1912".into(), DataType::Float64),
            Field::new("bhck1913".into(), DataType::Float64),
            Field::new("bhck1975".into(), DataType::Float64),
            Field::new("bhck2008".into(), DataType::Float64),
            Field::new("bhck2011".into(), DataType::Float64),
            Field::new("bhck2081".into(), DataType::Float64),
            Field::new("bhck2130".into(), DataType::Float64),
            Field::new("bhck2143".into(), DataType::Float64),
            Field::new("bhck2148".into(), DataType::Float64),
            Field::new("bhck2150".into(), DataType::Float64),
            Field::new("bhck2155".into(), DataType::Float64),
            Field::new("bhck2160".into(), DataType::Float64),
            Field::new("bhck2165".into(), DataType::Float64),
            Field::new("bhck2168".into(), DataType::Float64),
            Field::new("bhck2182".into(), DataType::Float64),
            Field::new("bhck2183".into(), DataType::Float64),
            Field::new("bhck2309".into(), DataType::Float64),
            Field::new("bhck2332".into(), DataType::Float64),
            Field::new("bhck2333".into(), DataType::Float64),
            Field::new("bhck2745".into(), DataType::Float64),
            Field::new("bhck2746".into(), DataType::Float64),
            Field::new("bhck2747".into(), DataType::Float64),
            Field::new("bhck2748".into(), DataType::Float64),
            Field::new("bhck2749".into(), DataType::Float64),
            Field::new("bhck2750".into(), DataType::Float64),
            Field::new("bhck2757".into(), DataType::Float64),
            Field::new("bhck2759".into(), DataType::Float64),
            Field::new("bhck2769".into(), DataType::Float64),
            Field::new("bhck2771".into(), DataType::Float64),
            Field::new("bhck2800".into(), DataType::Float64),
            Field::new("bhck2920".into(), DataType::Float64),
            Field::new("bhck3000".into(), DataType::Float64),
            Field::new("bhck3049".into(), DataType::Float64),
            Field::new("bhck3123".into(), DataType::Float64),
            Field::new("bhck3124".into(), DataType::Float64),
            Field::new("bhck3128".into(), DataType::Float64),
            Field::new("bhck3153".into(), DataType::Float64),
            Field::new("bhck3163".into(), DataType::Float64),
            Field::new("bhck3164".into(), DataType::Float64),
            Field::new("bhck3190".into(), DataType::Float64),
            Field::new("bhck3197".into(), DataType::Float64),
            Field::new("bhck3215".into(), DataType::Float64),
            Field::new("bhck3216".into(), DataType::Float64),
            Field::new("bhck3217".into(), DataType::Float64),
            Field::new("bhck3230".into(), DataType::Float64),
            Field::new("bhck3284".into(), DataType::Float64),
            Field::new("bhck3296".into(), DataType::Float64),
            Field::new("bhck3297".into(), DataType::Float64),
            Field::new("bhck3298".into(), DataType::Float64),
            Field::new("bhck3409".into(), DataType::Float64),
            Field::new("bhck3411".into(), DataType::Float64),
            Field::new("bhck3430".into(), DataType::Float64),
            Field::new("bhck3434".into(), DataType::Float64),
            Field::new("bhck3435".into(), DataType::Float64),
            Field::new("bhck3450".into(), DataType::Float64),
            Field::new("bhck3451".into(), DataType::Boolean),
            Field::new("bhck3452".into(), DataType::Boolean),
            Field::new("bhck3492".into(), DataType::Float64),
            Field::new("bhck3493".into(), DataType::Float64),
            Field::new("bhck3494".into(), DataType::Float64),
            Field::new("bhck3495".into(), DataType::Float64),
            Field::new("bhck3499".into(), DataType::Float64),
            Field::new("bhck3500".into(), DataType::Float64),
            Field::new("bhck3501".into(), DataType::Float64),
            Field::new("bhck3502".into(), DataType::Float64),
            Field::new("bhck3503".into(), DataType::Float64),
            Field::new("bhck3504".into(), DataType::Float64),
            Field::new("bhck3505".into(), DataType::Float64),
            Field::new("bhck3506".into(), DataType::Float64),
            Field::new("bhck3507".into(), DataType::Float64),
            Field::new("bhck3508".into(), DataType::Float64),
            Field::new("bhck3522".into(), DataType::Boolean),
            Field::new("bhck3528".into(), DataType::Float64),
            Field::new("bhck3529".into(), DataType::Float64),
            Field::new("bhck3530".into(), DataType::Float64),
            Field::new("bhck3541".into(), DataType::Float64),
            Field::new("bhck3546".into(), DataType::Float64),
            Field::new("bhck3571".into(), DataType::Float64),
            Field::new("bhck3572".into(), DataType::Float64),
            Field::new("bhck3574".into(), DataType::Float64),
            Field::new("bhck3576".into(), DataType::Float64),
            Field::new("bhck3578".into(), DataType::Float64),
            Field::new("bhck3580".into(), DataType::Float64),
            Field::new("bhck3581".into(), DataType::Float64),
            Field::new("bhck3582".into(), DataType::Float64),
            Field::new("bhck3584".into(), DataType::Float64),
            Field::new("bhck3588".into(), DataType::Float64),
            Field::new("bhck3590".into(), DataType::Float64),
            Field::new("bhck3656".into(), DataType::Float64),
            Field::new("bhck3806".into(), DataType::Float64),
            Field::new("bhck3809".into(), DataType::Float64),
            Field::new("bhck3812".into(), DataType::Float64),
            Field::new("bhck3816".into(), DataType::Float64),
            Field::new("bhck3820".into(), DataType::Float64),
            Field::new("bhck3822".into(), DataType::Float64),
            Field::new("bhck3826".into(), DataType::Float64),
            Field::new("bhck3836".into(), DataType::Float64),
            Field::new("bhck3837".into(), DataType::Float64),
            Field::new("bhck4010".into(), DataType::Float64),
            Field::new("bhck4020".into(), DataType::Float64),
            Field::new("bhck4027".into(), DataType::Float64),
            Field::new("bhck4042".into(), DataType::Float64),
            Field::new("bhck4059".into(), DataType::Float64),
            Field::new("bhck4060".into(), DataType::Float64),
            Field::new("bhck4065".into(), DataType::Float64),
            Field::new("bhck4069".into(), DataType::Float64),
            Field::new("bhck4070".into(), DataType::Float64),
            Field::new("bhck4074".into(), DataType::Float64),
            Field::new("bhck4078".into(), DataType::Float64),
            Field::new("bhck4092".into(), DataType::Float64),
            Field::new("bhck4105".into(), DataType::Float64),
            Field::new("bhck4106".into(), DataType::Float64),
            Field::new("bhck4115".into(), DataType::Float64),
            Field::new("bhck4136".into(), DataType::Float64),
            Field::new("bhck4141".into(), DataType::Float64),
            Field::new("bhck4146".into(), DataType::Float64),
            Field::new("bhck4150".into(), DataType::Float64),
            Field::new("bhck4172".into(), DataType::Float64),
            Field::new("bhck4180".into(), DataType::Float64),
            Field::new("bhck4185".into(), DataType::Float64),
            Field::new("bhck4217".into(), DataType::Float64),
            Field::new("bhck4219".into(), DataType::Float64),
            Field::new("bhck4300".into(), DataType::Float64),
            Field::new("bhck4301".into(), DataType::Float64),
            Field::new("bhck4302".into(), DataType::Float64),
            Field::new("bhck4313".into(), DataType::Float64),
            Field::new("bhck4320".into(), DataType::Float64),
            Field::new("bhck4336".into(), DataType::Float64),
            Field::new("bhck4340".into(), DataType::Float64),
            Field::new("bhck4356".into(), DataType::Float64),
            Field::new("bhck4393".into(), DataType::Float64),
            Field::new("bhck4394".into(), DataType::Float64),
            Field::new("bhck4395".into(), DataType::Float64),
            Field::new("bhck4396".into(), DataType::Float64),
            Field::new("bhck4397".into(), DataType::Float64),
            Field::new("bhck4398".into(), DataType::Float64),
            Field::new("bhck4399".into(), DataType::Float64),
            Field::new("bhck4411".into(), DataType::Float64),
            Field::new("bhck4412".into(), DataType::Float64),
            Field::new("bhck4414".into(), DataType::Float64),
            Field::new("bhck4435".into(), DataType::Float64),
            Field::new("bhck4436".into(), DataType::Float64),
            Field::new("bhck4460".into(), DataType::Float64),
            Field::new("bhck4484".into(), DataType::Float64),
            Field::new("bhck4503".into(), DataType::Float64),
            Field::new("bhck4504".into(), DataType::Float64),
            Field::new("bhck4506".into(), DataType::Float64),
            Field::new("bhck4507".into(), DataType::Float64),
            Field::new("bhck4518".into(), DataType::Float64),
            Field::new("bhck4519".into(), DataType::Float64),
            Field::new("bhck4531".into(), DataType::Float64),
            Field::new("bhck4574".into(), DataType::Float64),
            Field::new("bhck4591".into(), DataType::Float64),
            Field::new("bhck4592".into(), DataType::Float64),
            Field::new("bhck4598".into(), DataType::Float64),
            Field::new("bhck4635".into(), DataType::Float64),
            Field::new("bhck4643".into(), DataType::Float64),
            Field::new("bhck4644".into(), DataType::Float64),
            Field::new("bhck4645".into(), DataType::Float64),
            Field::new("bhck4646".into(), DataType::Float64),
            Field::new("bhck4651".into(), DataType::Float64),
            Field::new("bhck4652".into(), DataType::Float64),
            Field::new("bhck4653".into(), DataType::Float64),
            Field::new("bhck4654".into(), DataType::Float64),
            Field::new("bhck4655".into(), DataType::Float64),
            Field::new("bhck4656".into(), DataType::Float64),
            Field::new("bhck4657".into(), DataType::Float64),
            Field::new("bhck4658".into(), DataType::Float64),
            Field::new("bhck4659".into(), DataType::Float64),
            Field::new("bhck4776".into(), DataType::Float64),
            Field::new("bhck4815".into(), DataType::Float64),
            Field::new("bhck4832".into(), DataType::Float64),
            Field::new("bhck4833".into(), DataType::Float64),
            Field::new("bhck4834".into(), DataType::Float64),
            Field::new("bhck5041".into(), DataType::Float64),
            Field::new("bhck5043".into(), DataType::Float64),
            Field::new("bhck5045".into(), DataType::Float64),
            Field::new("bhck5047".into(), DataType::Float64),
            Field::new("bhck5310".into(), DataType::Float64),
            Field::new("bhck5351".into(), DataType::Float64),
            Field::new("bhck5354".into(), DataType::Float64),
            Field::new("bhck5355".into(), DataType::Float64),
            Field::new("bhck5356".into(), DataType::Float64),
            Field::new("bhck5359".into(), DataType::Float64),
            Field::new("bhck5360".into(), DataType::Float64),
            Field::new("bhck5369".into(), DataType::Float64),
            Field::new("bhck5377".into(), DataType::Float64),
            Field::new("bhck5378".into(), DataType::Float64),
            Field::new("bhck5379".into(), DataType::Float64),
            Field::new("bhck5380".into(), DataType::Float64),
            Field::new("bhck5381".into(), DataType::Float64),
            Field::new("bhck5382".into(), DataType::Float64),
            Field::new("bhck5383".into(), DataType::Boolean),
            Field::new("bhck5384".into(), DataType::Float64),
            Field::new("bhck5385".into(), DataType::Float64),
            Field::new("bhck5386".into(), DataType::Boolean),
            Field::new("bhck5387".into(), DataType::Float64),
            Field::new("bhck5388".into(), DataType::Float64),
            Field::new("bhck5389".into(), DataType::Float64),
            Field::new("bhck5390".into(), DataType::Float64),
            Field::new("bhck5391".into(), DataType::Float64),
            Field::new("bhck5393".into(), DataType::Float64),
            Field::new("bhck5397".into(), DataType::Float64),
            Field::new("bhck5398".into(), DataType::Float64),
            Field::new("bhck5399".into(), DataType::Float64),
            Field::new("bhck5400".into(), DataType::Float64),
            Field::new("bhck5401".into(), DataType::Float64),
            Field::new("bhck5402".into(), DataType::Float64),
            Field::new("bhck5403".into(), DataType::Float64),
            Field::new("bhck5409".into(), DataType::Float64),
            Field::new("bhck5411".into(), DataType::Float64),
            Field::new("bhck5413".into(), DataType::Float64),
            Field::new("bhck5459".into(), DataType::Float64),
            Field::new("bhck5460".into(), DataType::Float64),
            Field::new("bhck5461".into(), DataType::Float64),
            Field::new("bhck5507".into(), DataType::Float64),
            Field::new("bhck5610".into(), DataType::Float64),
            Field::new("bhck5612".into(), DataType::Float64),
            Field::new("bhck5613".into(), DataType::Float64),
            Field::new("bhck5614".into(), DataType::Float64),
            Field::new("bhck5615".into(), DataType::Float64),
            Field::new("bhck5616".into(), DataType::Float64),
            Field::new("bhck5617".into(), DataType::Float64),
            Field::new("bhck6019".into(), DataType::Float64),
            Field::new("bhck6373".into(), DataType::Float64),
            Field::new("bhck6416".into(), DataType::Float64),
            Field::new("bhck6438".into(), DataType::Float64),
            Field::new("bhck6441".into(), DataType::Float64),
            Field::new("bhck6442".into(), DataType::Float64),
            Field::new("bhck6550".into(), DataType::Float64),
            Field::new("bhck6555".into(), DataType::Float64),
            Field::new("bhck6556".into(), DataType::Float64),
            Field::new("bhck6557".into(), DataType::Float64),
            Field::new("bhck6558".into(), DataType::Float64),
            Field::new("bhck6559".into(), DataType::Float64),
            Field::new("bhck6560".into(), DataType::Float64),
            Field::new("bhck6561".into(), DataType::Float64),
            Field::new("bhck6566".into(), DataType::Float64),
            Field::new("bhck6572".into(), DataType::Float64),
            Field::new("bhck6586".into(), DataType::Float64),
            Field::new("bhck6599".into(), DataType::Float64),
            Field::new("bhck6649".into(), DataType::Float64),
            Field::new("bhck6669".into(), DataType::Boolean),
            Field::new("bhck6688".into(), DataType::Float64),
            Field::new("bhck6689".into(), DataType::Float64),
            Field::new("bhck6761".into(), DataType::Float64),
            Field::new("bhck6765".into(), DataType::Float64),
            Field::new("bhck6927".into(), DataType::Boolean),
            Field::new("bhck6928".into(), DataType::Boolean),
            Field::new("bhck6995".into(), DataType::Boolean),
            Field::new("bhck6998".into(), DataType::Boolean),
            Field::new("bhck8403".into(), DataType::Float64),
            Field::new("bhck8427".into(), DataType::Float64),
            Field::new("bhck8428".into(), DataType::Float64),
            Field::new("bhck8429".into(), DataType::Float64),
            Field::new("bhck8430".into(), DataType::Float64),
            Field::new("bhck8431".into(), DataType::Float64),
            Field::new("bhck8433".into(), DataType::Float64),
            Field::new("bhck8434".into(), DataType::Float64),
            Field::new("bhck8492".into(), DataType::Float64),
            Field::new("bhck8493".into(), DataType::Float64),
            Field::new("bhck8494".into(), DataType::Float64),
            Field::new("bhck8495".into(), DataType::Float64),
            Field::new("bhck8496".into(), DataType::Float64),
            Field::new("bhck8497".into(), DataType::Float64),
            Field::new("bhck8498".into(), DataType::Float64),
            Field::new("bhck8499".into(), DataType::Float64),
            Field::new("bhck8531".into(), DataType::Float64),
            Field::new("bhck8532".into(), DataType::Float64),
            Field::new("bhck8533".into(), DataType::Float64),
            Field::new("bhck8534".into(), DataType::Float64),
            Field::new("bhck8535".into(), DataType::Float64),
            Field::new("bhck8536".into(), DataType::Float64),
            Field::new("bhck8537".into(), DataType::Float64),
            Field::new("bhck8538".into(), DataType::Float64),
            Field::new("bhck8539".into(), DataType::Float64),
            Field::new("bhck8540".into(), DataType::Float64),
            Field::new("bhck8541".into(), DataType::Float64),
            Field::new("bhck8542".into(), DataType::Float64),
            Field::new("bhck8543".into(), DataType::Float64),
            Field::new("bhck8544".into(), DataType::Float64),
            Field::new("bhck8545".into(), DataType::Float64),
            Field::new("bhck8546".into(), DataType::Float64),
            Field::new("bhck8547".into(), DataType::Float64),
            Field::new("bhck8548".into(), DataType::Float64),
            Field::new("bhck8549".into(), DataType::Float64),
            Field::new("bhck8550".into(), DataType::Float64),
            Field::new("bhck8557".into(), DataType::Float64),
            Field::new("bhck8558".into(), DataType::Float64),
            Field::new("bhck8559".into(), DataType::Float64),
            Field::new("bhck8560".into(), DataType::Float64),
            Field::new("bhck8561".into(), DataType::Float64),
            Field::new("bhck8562".into(), DataType::Float64),
            Field::new("bhck8563".into(), DataType::Float64),
            Field::new("bhck8564".into(), DataType::Float64),
            Field::new("bhck8565".into(), DataType::Float64),
            Field::new("bhck8566".into(), DataType::Float64),
            Field::new("bhck8567".into(), DataType::Float64),
            Field::new("bhck8693".into(), DataType::Float64),
            Field::new("bhck8694".into(), DataType::Float64),
            Field::new("bhck8695".into(), DataType::Float64),
            Field::new("bhck8696".into(), DataType::Float64),
            Field::new("bhck8697".into(), DataType::Float64),
            Field::new("bhck8698".into(), DataType::Float64),
            Field::new("bhck8699".into(), DataType::Float64),
            Field::new("bhck8700".into(), DataType::Float64),
            Field::new("bhck8719".into(), DataType::Float64),
            Field::new("bhck8720".into(), DataType::Float64),
            Field::new("bhck8733".into(), DataType::Float64),
            Field::new("bhck8734".into(), DataType::Float64),
            Field::new("bhck8735".into(), DataType::Float64),
            Field::new("bhck8736".into(), DataType::Float64),
            Field::new("bhck8737".into(), DataType::Float64),
            Field::new("bhck8738".into(), DataType::Float64),
            Field::new("bhck8739".into(), DataType::Float64),
            Field::new("bhck8740".into(), DataType::Float64),
            Field::new("bhck8741".into(), DataType::Float64),
            Field::new("bhck8742".into(), DataType::Float64),
            Field::new("bhck8743".into(), DataType::Float64),
            Field::new("bhck8744".into(), DataType::Float64),
            Field::new("bhck8745".into(), DataType::Float64),
            Field::new("bhck8746".into(), DataType::Float64),
            Field::new("bhck8747".into(), DataType::Float64),
            Field::new("bhck8748".into(), DataType::Float64),
            Field::new("bhck8749".into(), DataType::Float64),
            Field::new("bhck8750".into(), DataType::Float64),
            Field::new("bhck8751".into(), DataType::Float64),
            Field::new("bhck8752".into(), DataType::Float64),
            Field::new("bhck8753".into(), DataType::Float64),
            Field::new("bhck8754".into(), DataType::Float64),
            Field::new("bhck8755".into(), DataType::Float64),
            Field::new("bhck8756".into(), DataType::Float64),
            Field::new("bhck8757".into(), DataType::Float64),
            Field::new("bhck8758".into(), DataType::Float64),
            Field::new("bhck8759".into(), DataType::Float64),
            Field::new("bhck8760".into(), DataType::Float64),
            Field::new("bhck8761".into(), DataType::Float64),
            Field::new("bhck8762".into(), DataType::Float64),
            Field::new("bhck8763".into(), DataType::Float64),
            Field::new("bhck8764".into(), DataType::Float64),
            Field::new("bhck8766".into(), DataType::Float64),
            Field::new("bhck8767".into(), DataType::Float64),
            Field::new("bhck8769".into(), DataType::Float64),
            Field::new("bhck8770".into(), DataType::Float64),
            Field::new("bhck8771".into(), DataType::Float64),
            Field::new("bhck8772".into(), DataType::Float64),
            Field::new("bhck8773".into(), DataType::Float64),
            Field::new("bhck8774".into(), DataType::Float64),
            Field::new("bhck8775".into(), DataType::Float64),
            Field::new("bhck8776".into(), DataType::Float64),
            Field::new("bhck8777".into(), DataType::Float64),
            Field::new("bhck8778".into(), DataType::Float64),
            Field::new("bhck8779".into(), DataType::Float64),
            Field::new("bhck8782".into(), DataType::Float64),
            Field::new("bhck8783".into(), DataType::Float64),
            Field::new("bhck8843".into(), DataType::Float64),
            Field::new("bhcka000".into(), DataType::Float64),
            Field::new("bhcka001".into(), DataType::Float64),
            Field::new("bhcka002".into(), DataType::Float64),
            Field::new("bhcka130".into(), DataType::Float64),
            Field::new("bhcka221".into(), DataType::Float64),
            Field::new("bhcka222".into(), DataType::Float64),
            Field::new("bhcka224".into(), DataType::Float64),
            Field::new("bhcka250".into(), DataType::Float64),
            Field::new("bhcka251".into(), DataType::Float64),
            Field::new("bhcka506".into(), DataType::Float64),
            Field::new("bhcka507".into(), DataType::Float64),
            Field::new("bhcka510".into(), DataType::Float64),
            Field::new("bhcka511".into(), DataType::Float64),
            Field::new("bhcka512".into(), DataType::Float64),
            Field::new("bhcka517".into(), DataType::Float64),
            Field::new("bhcka518".into(), DataType::Float64),
            Field::new("bhcka519".into(), DataType::Float64),
            Field::new("bhcka520".into(), DataType::Float64),
            Field::new("bhcka521".into(), DataType::Float64),
            Field::new("bhcka522".into(), DataType::Float64),
            Field::new("bhcka523".into(), DataType::Float64),
            Field::new("bhcka524".into(), DataType::Float64),
            Field::new("bhcka525".into(), DataType::Float64),
            Field::new("bhcka530".into(), DataType::Float64),
            Field::new("bhcka534".into(), DataType::Float64),
            Field::new("bhcka535".into(), DataType::Float64),
            Field::new("bhckb026".into(), DataType::Float64),
            Field::new("bhckb029".into(), DataType::Float64),
            Field::new("bhckb030".into(), DataType::Float64),
            Field::new("bhckb032".into(), DataType::Float64),
            Field::new("bhckb035".into(), DataType::Float64),
            Field::new("bhckb036".into(), DataType::Float64),
            Field::new("bhckb039".into(), DataType::Float64),
            Field::new("bhckb040".into(), DataType::Float64),
            Field::new("bhckb044".into(), DataType::Float64),
            Field::new("bhckb045".into(), DataType::Float64),
            Field::new("bhckb047".into(), DataType::Float64),
            Field::new("bhckb050".into(), DataType::Float64),
            Field::new("bhckb051".into(), DataType::Float64),
            Field::new("bhckb054".into(), DataType::Float64),
            Field::new("bhckb055".into(), DataType::Float64),
            Field::new("bhckb077".into(), DataType::Float64),
            Field::new("bhckb488".into(), DataType::Float64),
            Field::new("bhckb489".into(), DataType::Float64),
            Field::new("bhckb490".into(), DataType::Float64),
            Field::new("bhckb492".into(), DataType::Float64),
            Field::new("bhckb493".into(), DataType::Float64),
            Field::new("bhckb494".into(), DataType::Float64),
            Field::new("bhckb496".into(), DataType::Float64),
            Field::new("bhckb497".into(), DataType::Float64),
            Field::new("bhckb500".into(), DataType::Float64),
            Field::new("bhckb501".into(), DataType::Float64),
            Field::new("bhckb502".into(), DataType::Float64),
            Field::new("bhckb508".into(), DataType::Float64),
            Field::new("bhckb511".into(), DataType::Float64),
            Field::new("bhckb512".into(), DataType::Float64),
            Field::new("bhckb514".into(), DataType::Float64),
            Field::new("bhckb516".into(), DataType::Float64),
            Field::new("bhckb522".into(), DataType::Float64),
            Field::new("bhckb528".into(), DataType::Float64),
            Field::new("bhckb529".into(), DataType::Float64),
            Field::new("bhckb530".into(), DataType::Float64),
            Field::new("bhckb538".into(), DataType::Float64),
            Field::new("bhckb539".into(), DataType::Float64),
            Field::new("bhckb546".into(), DataType::Float64),
            Field::new("bhckb556".into(), DataType::Float64),
            Field::new("bhckb557".into(), DataType::Float64),
            Field::new("bhckb559".into(), DataType::Float64),
            Field::new("bhckb560".into(), DataType::Float64),
            Field::new("bhckb569".into(), DataType::Float64),
            Field::new("bhckb570".into(), DataType::Float64),
            Field::new("bhckb572".into(), DataType::Float64),
            Field::new("bhckb573".into(), DataType::Float64),
            Field::new("bhckb574".into(), DataType::Float64),
            Field::new("bhckb575".into(), DataType::Float64),
            Field::new("bhckb576".into(), DataType::Float64),
            Field::new("bhckb577".into(), DataType::Float64),
            Field::new("bhckb578".into(), DataType::Float64),
            Field::new("bhckb579".into(), DataType::Float64),
            Field::new("bhckb580".into(), DataType::Float64),
            Field::new("bhckb588".into(), DataType::Float64),
            Field::new("bhckb590".into(), DataType::Float64),
            Field::new("bhckb591".into(), DataType::Float64),
            Field::new("bhckb592".into(), DataType::Float64),
            Field::new("bhckb593".into(), DataType::Float64),
            Field::new("bhckb594".into(), DataType::Float64),
            Field::new("bhckb595".into(), DataType::Float64),
            Field::new("bhckb596".into(), DataType::Float64),
            Field::new("bhckb639".into(), DataType::Float64),
            Field::new("bhckb675".into(), DataType::Float64),
            Field::new("bhckb681".into(), DataType::Float64),
            Field::new("bhckb747".into(), DataType::Float64),
            Field::new("bhckb748".into(), DataType::Float64),
            Field::new("bhckb749".into(), DataType::Float64),
            Field::new("bhckb750".into(), DataType::Float64),
            Field::new("bhckb751".into(), DataType::Float64),
            Field::new("bhckb752".into(), DataType::Float64),
            Field::new("bhckb753".into(), DataType::Float64),
            Field::new("bhckb761".into(), DataType::Float64),
            Field::new("bhckb762".into(), DataType::Float64),
            Field::new("bhckb763".into(), DataType::Float64),
            Field::new("bhckb770".into(), DataType::Float64),
            Field::new("bhckb771".into(), DataType::Float64),
            Field::new("bhckb772".into(), DataType::Float64),
            Field::new("bhckb776".into(), DataType::Float64),
            Field::new("bhckb777".into(), DataType::Float64),
            Field::new("bhckb778".into(), DataType::Float64),
            Field::new("bhckb779".into(), DataType::Float64),
            Field::new("bhckb780".into(), DataType::Float64),
            Field::new("bhckb781".into(), DataType::Float64),
            Field::new("bhckb782".into(), DataType::Float64),
            Field::new("bhckb790".into(), DataType::Float64),
            Field::new("bhckb791".into(), DataType::Float64),
            Field::new("bhckb792".into(), DataType::Float64),
            Field::new("bhckb793".into(), DataType::Float64),
            Field::new("bhckb794".into(), DataType::Float64),
            Field::new("bhckb795".into(), DataType::Float64),
            Field::new("bhckb796".into(), DataType::Float64),
            Field::new("bhckb797".into(), DataType::Float64),
            Field::new("bhckb798".into(), DataType::Float64),
            Field::new("bhckb799".into(), DataType::Float64),
            Field::new("bhckb800".into(), DataType::Float64),
            Field::new("bhckb801".into(), DataType::Float64),
            Field::new("bhckb802".into(), DataType::Float64),
            Field::new("bhckb803".into(), DataType::Float64),
            Field::new("bhckb806".into(), DataType::Float64),
            Field::new("bhckb807".into(), DataType::Float64),
            Field::new("bhckb837".into(), DataType::Float64),
            Field::new("bhckb838".into(), DataType::Float64),
            Field::new("bhckb839".into(), DataType::Float64),
            Field::new("bhckb840".into(), DataType::Float64),
            Field::new("bhckb841".into(), DataType::Float64),
            Field::new("bhckb842".into(), DataType::Float64),
            Field::new("bhckb843".into(), DataType::Float64),
            Field::new("bhckb844".into(), DataType::Float64),
            Field::new("bhckb845".into(), DataType::Float64),
            Field::new("bhckb846".into(), DataType::Float64),
            Field::new("bhckb847".into(), DataType::Float64),
            Field::new("bhckb848".into(), DataType::Float64),
            Field::new("bhckb849".into(), DataType::Float64),
            Field::new("bhckb850".into(), DataType::Float64),
            Field::new("bhckb851".into(), DataType::Float64),
            Field::new("bhckb852".into(), DataType::Float64),
            Field::new("bhckb853".into(), DataType::Float64),
            Field::new("bhckb854".into(), DataType::Float64),
            Field::new("bhckb855".into(), DataType::Float64),
            Field::new("bhckb856".into(), DataType::Float64),
            Field::new("bhckb857".into(), DataType::Float64),
            Field::new("bhckb858".into(), DataType::Float64),
            Field::new("bhckb859".into(), DataType::Float64),
            Field::new("bhckb860".into(), DataType::Float64),
            Field::new("bhckb861".into(), DataType::Float64),
            Field::new("bhckb983".into(), DataType::Float64),
            Field::new("bhckb984".into(), DataType::Float64),
            Field::new("bhckb985".into(), DataType::Float64),
            Field::new("bhckb986".into(), DataType::Boolean),
            Field::new("bhckb988".into(), DataType::Float64),
            Field::new("bhckb990".into(), DataType::Float64),
            Field::new("bhckb991".into(), DataType::Float64),
            Field::new("bhckb992".into(), DataType::Float64),
            Field::new("bhckb994".into(), DataType::Float64),
            Field::new("bhckb996".into(), DataType::Float64),
            Field::new("bhckb998".into(), DataType::Float64),
            Field::new("bhckc009".into(), DataType::Float64),
            Field::new("bhckc013".into(), DataType::Float64),
            Field::new("bhckc014".into(), DataType::Float64),
            Field::new("bhckc016".into(), DataType::Float64),
            Field::new("bhckc017".into(), DataType::Float64),
            Field::new("bhckc050".into(), DataType::Boolean),
            Field::new("bhckc079".into(), DataType::Float64),
            Field::new("bhckc159".into(), DataType::Float64),
            Field::new("bhckc160".into(), DataType::Float64),
            Field::new("bhckc161".into(), DataType::Float64),
            Field::new("bhckc216".into(), DataType::Float64),
            Field::new("bhckc219".into(), DataType::Float64),
            Field::new("bhckc220".into(), DataType::Float64),
            Field::new("bhckc221".into(), DataType::Float64),
            Field::new("bhckc222".into(), DataType::Float64),
            Field::new("bhckc225".into(), DataType::Float64),
            Field::new("bhckc226".into(), DataType::Float64),
            Field::new("bhckc229".into(), DataType::Float64),
            Field::new("bhckc230".into(), DataType::Float64),
            Field::new("bhckc231".into(), DataType::Float64),
            Field::new("bhckc232".into(), DataType::Float64),
            Field::new("bhckc233".into(), DataType::Float64),
            Field::new("bhckc234".into(), DataType::Float64),
            Field::new("bhckc235".into(), DataType::Float64),
            Field::new("bhckc236".into(), DataType::Float64),
            Field::new("bhckc237".into(), DataType::Float64),
            Field::new("bhckc238".into(), DataType::Float64),
            Field::new("bhckc239".into(), DataType::Float64),
            Field::new("bhckc240".into(), DataType::Float64),
            Field::new("bhckc241".into(), DataType::Float64),
            Field::new("bhckc243".into(), DataType::Float64),
            Field::new("bhckc246".into(), DataType::Float64),
            Field::new("bhckc250".into(), DataType::Float64),
            Field::new("bhckc251".into(), DataType::Float64),
            Field::new("bhckc252".into(), DataType::Float64),
            Field::new("bhckc253".into(), DataType::Float64),
            Field::new("bhckc386".into(), DataType::Float64),
            Field::new("bhckc387".into(), DataType::Float64),
            Field::new("bhckc390".into(), DataType::Float64),
            Field::new("bhckc410".into(), DataType::Float64),
            Field::new("bhckc411".into(), DataType::Float64),
            Field::new("bhckc435".into(), DataType::Float64),
            Field::new("bhckc447".into(), DataType::Float64),
            Field::new("bhckc498".into(), DataType::Float64),
            Field::new("bhckc700".into(), DataType::Float64),
            Field::new("bhckc701".into(), DataType::Float64),
            Field::new("bhckc781".into(), DataType::Float64),
            Field::new("bhckc880".into(), DataType::Float64),
            Field::new("bhckc884".into(), DataType::Float64),
            Field::new("bhckc886".into(), DataType::Float64),
            Field::new("bhckc887".into(), DataType::Float64),
            Field::new("bhckc888".into(), DataType::Float64),
            Field::new("bhckc889".into(), DataType::Float64),
            Field::new("bhckc890".into(), DataType::Float64),
            Field::new("bhckc891".into(), DataType::Float64),
            Field::new("bhckc892".into(), DataType::Float64),
            Field::new("bhckc893".into(), DataType::Float64),
            Field::new("bhckc894".into(), DataType::Float64),
            Field::new("bhckc895".into(), DataType::Float64),
            Field::new("bhckc896".into(), DataType::Float64),
            Field::new("bhckc897".into(), DataType::Float64),
            Field::new("bhckc898".into(), DataType::Float64),
            Field::new("bhckc968".into(), DataType::Float64),
            Field::new("bhckc969".into(), DataType::Float64),
            Field::new("bhckc970".into(), DataType::Float64),
            Field::new("bhckc971".into(), DataType::Float64),
            Field::new("bhckc972".into(), DataType::Float64),
            Field::new("bhckc973".into(), DataType::Float64),
            Field::new("bhckc974".into(), DataType::Float64),
            Field::new("bhckc975".into(), DataType::Float64),
            Field::new("bhckc980".into(), DataType::Float64),
            Field::new("bhckc981".into(), DataType::Float64),
            Field::new("bhckc982".into(), DataType::Float64),
            Field::new("bhckc983".into(), DataType::Float64),
            Field::new("bhckc984".into(), DataType::Float64),
            Field::new("bhckc985".into(), DataType::Float64),
            Field::new("bhckc988".into(), DataType::Float64),
            Field::new("bhckc989".into(), DataType::Float64),
            Field::new("bhckd958".into(), DataType::Float64),
            Field::new("bhckd959".into(), DataType::Float64),
            Field::new("bhckd960".into(), DataType::Float64),
            Field::new("bhckd962".into(), DataType::Float64),
            Field::new("bhckd963".into(), DataType::Float64),
            Field::new("bhckd964".into(), DataType::Float64),
            Field::new("bhckd965".into(), DataType::Float64),
            Field::new("bhckd967".into(), DataType::Float64),
            Field::new("bhckd968".into(), DataType::Float64),
            Field::new("bhckd969".into(), DataType::Float64),
            Field::new("bhckd970".into(), DataType::Float64),
            Field::new("bhckd971".into(), DataType::Float64),
            Field::new("bhckd972".into(), DataType::Float64),
            Field::new("bhckd973".into(), DataType::Float64),
            Field::new("bhckd974".into(), DataType::Float64),
            Field::new("bhckd982".into(), DataType::Float64),
            Field::new("bhckd983".into(), DataType::Float64),
            Field::new("bhckd984".into(), DataType::Float64),
            Field::new("bhckd985".into(), DataType::Float64),
            Field::new("bhckd991".into(), DataType::Float64),
            Field::new("bhckd992".into(), DataType::Float64),
            Field::new("bhckd993".into(), DataType::Float64),
            Field::new("bhckd994".into(), DataType::Float64),
            Field::new("bhckd995".into(), DataType::Float64),
            Field::new("bhckd996".into(), DataType::Float64),
            Field::new("bhckf031".into(), DataType::Float64),
            Field::new("bhckf070".into(), DataType::Float64),
            Field::new("bhckf071".into(), DataType::Float64),
            Field::new("bhckf072".into(), DataType::Float64),
            Field::new("bhckf073".into(), DataType::Float64),
            Field::new("bhckf158".into(), DataType::Float64),
            Field::new("bhckf159".into(), DataType::Float64),
            Field::new("bhckf160".into(), DataType::Float64),
            Field::new("bhckf161".into(), DataType::Float64),
            Field::new("bhckf162".into(), DataType::Float64),
            Field::new("bhckf163".into(), DataType::Float64),
            Field::new("bhckf164".into(), DataType::Float64),
            Field::new("bhckf165".into(), DataType::Float64),
            Field::new("bhckf166".into(), DataType::Float64),
            Field::new("bhckf167".into(), DataType::Float64),
            Field::new("bhckf168".into(), DataType::Float64),
            Field::new("bhckf169".into(), DataType::Float64),
            Field::new("bhckf170".into(), DataType::Float64),
            Field::new("bhckf171".into(), DataType::Float64),
            Field::new("bhckf172".into(), DataType::Float64),
            Field::new("bhckf173".into(), DataType::Float64),
            Field::new("bhckf174".into(), DataType::Float64),
            Field::new("bhckf175".into(), DataType::Float64),
            Field::new("bhckf176".into(), DataType::Float64),
            Field::new("bhckf177".into(), DataType::Float64),
            Field::new("bhckf178".into(), DataType::Float64),
            Field::new("bhckf179".into(), DataType::Float64),
            Field::new("bhckf180".into(), DataType::Float64),
            Field::new("bhckf181".into(), DataType::Float64),
            Field::new("bhckf182".into(), DataType::Float64),
            Field::new("bhckf183".into(), DataType::Float64),
            Field::new("bhckf184".into(), DataType::Float64),
            Field::new("bhckf185".into(), DataType::Float64),
            Field::new("bhckf228".into(), DataType::Float64),
            Field::new("bhckf229".into(), DataType::Float64),
            Field::new("bhckf241".into(), DataType::Float64),
            Field::new("bhckf242".into(), DataType::Float64),
            Field::new("bhckf244".into(), DataType::Float64),
            Field::new("bhckf245".into(), DataType::Float64),
            Field::new("bhckf247".into(), DataType::Float64),
            Field::new("bhckf248".into(), DataType::Float64),
            Field::new("bhckf250".into(), DataType::Float64),
            Field::new("bhckf251".into(), DataType::Float64),
            Field::new("bhckf253".into(), DataType::Float64),
            Field::new("bhckf254".into(), DataType::Float64),
            Field::new("bhckf256".into(), DataType::Float64),
            Field::new("bhckf257".into(), DataType::Float64),
            Field::new("bhckf259".into(), DataType::Float64),
            Field::new("bhckf260".into(), DataType::Float64),
            Field::new("bhckf262".into(), DataType::Float64),
            Field::new("bhckf263".into(), DataType::Float64),
            Field::new("bhckf264".into(), DataType::Float64),
            Field::new("bhckf465".into(), DataType::Float64),
            Field::new("bhckf551".into(), DataType::Float64),
            Field::new("bhckf552".into(), DataType::Float64),
            Field::new("bhckf553".into(), DataType::Float64),
            Field::new("bhckf554".into(), DataType::Float64),
            Field::new("bhckf555".into(), DataType::Float64),
            Field::new("bhckf556".into(), DataType::Float64),
            Field::new("bhckf557".into(), DataType::Float64),
            Field::new("bhckf558".into(), DataType::Float64),
            Field::new("bhckf585".into(), DataType::Float64),
            Field::new("bhckf586".into(), DataType::Float64),
            Field::new("bhckf587".into(), DataType::Float64),
            Field::new("bhckf588".into(), DataType::Float64),
            Field::new("bhckf589".into(), DataType::Float64),
            Field::new("bhckf608".into(), DataType::Float64),
            Field::new("bhckf639".into(), DataType::Float64),
            Field::new("bhckf640".into(), DataType::Float64),
            Field::new("bhckf655".into(), DataType::Float64),
            Field::new("bhckf658".into(), DataType::Float64),
            Field::new("bhckf661".into(), DataType::Float64),
            Field::new("bhckf662".into(), DataType::Float64),
            Field::new("bhckf663".into(), DataType::Float64),
            Field::new("bhckf664".into(), DataType::Float64),
            Field::new("bhckf665".into(), DataType::Float64),
            Field::new("bhckf666".into(), DataType::Float64),
            Field::new("bhckf682".into(), DataType::Float64),
            Field::new("bhckf683".into(), DataType::Float64),
            Field::new("bhckf684".into(), DataType::Float64),
            Field::new("bhckf685".into(), DataType::Float64),
            Field::new("bhckf686".into(), DataType::Float64),
            Field::new("bhckf687".into(), DataType::Float64),
            Field::new("bhckf688".into(), DataType::Float64),
            Field::new("bhckf689".into(), DataType::Float64),
            Field::new("bhckf690".into(), DataType::Float64),
            Field::new("bhckf691".into(), DataType::Float64),
            Field::new("bhckf692".into(), DataType::Float64),
            Field::new("bhckf693".into(), DataType::Float64),
            Field::new("bhckf694".into(), DataType::Float64),
            Field::new("bhckf695".into(), DataType::Float64),
            Field::new("bhckf696".into(), DataType::Float64),
            Field::new("bhckf697".into(), DataType::Float64),
            Field::new("bhckf821".into(), DataType::Float64),
            Field::new("bhckf841".into(), DataType::Boolean),
            Field::new("bhckft28".into(), DataType::Float64),
            Field::new("bhckft29".into(), DataType::Float64),
            Field::new("bhckft30".into(), DataType::Float64),
            Field::new("bhckft31".into(), DataType::Float64),
            Field::new("bhckft32".into(), DataType::Float64),
            Field::new("bhckft41".into(), DataType::Float64),
            Field::new("bhckft42".into(), DataType::Boolean),
            Field::new("bhckft43".into(), DataType::Boolean),
            Field::new("bhckft44".into(), DataType::Boolean),
            Field::new("bhckg091".into(), DataType::Float64),
            Field::new("bhckg092".into(), DataType::Float64),
            Field::new("bhckg093".into(), DataType::Float64),
            Field::new("bhckg094".into(), DataType::Float64),
            Field::new("bhckg095".into(), DataType::Float64),
            Field::new("bhckg096".into(), DataType::Float64),
            Field::new("bhckg097".into(), DataType::Float64),
            Field::new("bhckg098".into(), DataType::Float64),
            Field::new("bhckg099".into(), DataType::Float64),
            Field::new("bhckg100".into(), DataType::Float64),
            Field::new("bhckg101".into(), DataType::Float64),
            Field::new("bhckg102".into(), DataType::Float64),
            Field::new("bhckg103".into(), DataType::Float64),
            Field::new("bhckg104".into(), DataType::Float64),
            Field::new("bhckg209".into(), DataType::Float64),
            Field::new("bhckg210".into(), DataType::Float64),
            Field::new("bhckg211".into(), DataType::Float64),
            Field::new("bhckg212".into(), DataType::Float64),
            Field::new("bhckg213".into(), DataType::Float64),
            Field::new("bhckg218".into(), DataType::Float64),
            Field::new("bhckg221".into(), DataType::Float64),
            Field::new("bhckg234".into(), DataType::Float64),
            Field::new("bhckg235".into(), DataType::Float64),
            Field::new("bhckg300".into(), DataType::Float64),
            Field::new("bhckg301".into(), DataType::Float64),
            Field::new("bhckg302".into(), DataType::Float64),
            Field::new("bhckg303".into(), DataType::Float64),
            Field::new("bhckg304".into(), DataType::Float64),
            Field::new("bhckg305".into(), DataType::Float64),
            Field::new("bhckg306".into(), DataType::Float64),
            Field::new("bhckg307".into(), DataType::Float64),
            Field::new("bhckg308".into(), DataType::Float64),
            Field::new("bhckg309".into(), DataType::Float64),
            Field::new("bhckg310".into(), DataType::Float64),
            Field::new("bhckg311".into(), DataType::Float64),
            Field::new("bhckg312".into(), DataType::Float64),
            Field::new("bhckg313".into(), DataType::Float64),
            Field::new("bhckg314".into(), DataType::Float64),
            Field::new("bhckg315".into(), DataType::Float64),
            Field::new("bhckg316".into(), DataType::Float64),
            Field::new("bhckg317".into(), DataType::Float64),
            Field::new("bhckg318".into(), DataType::Float64),
            Field::new("bhckg319".into(), DataType::Float64),
            Field::new("bhckg320".into(), DataType::Float64),
            Field::new("bhckg321".into(), DataType::Float64),
            Field::new("bhckg322".into(), DataType::Float64),
            Field::new("bhckg323".into(), DataType::Float64),
            Field::new("bhckg324".into(), DataType::Float64),
            Field::new("bhckg325".into(), DataType::Float64),
            Field::new("bhckg326".into(), DataType::Float64),
            Field::new("bhckg327".into(), DataType::Float64),
            Field::new("bhckg328".into(), DataType::Float64),
            Field::new("bhckg329".into(), DataType::Float64),
            Field::new("bhckg330".into(), DataType::Float64),
            Field::new("bhckg331".into(), DataType::Float64),
            Field::new("bhckg336".into(), DataType::Float64),
            Field::new("bhckg337".into(), DataType::Float64),
            Field::new("bhckg338".into(), DataType::Float64),
            Field::new("bhckg339".into(), DataType::Float64),
            Field::new("bhckg340".into(), DataType::Float64),
            Field::new("bhckg341".into(), DataType::Float64),
            Field::new("bhckg342".into(), DataType::Float64),
            Field::new("bhckg343".into(), DataType::Float64),
            Field::new("bhckg344".into(), DataType::Float64),
            Field::new("bhckg345".into(), DataType::Float64),
            Field::new("bhckg346".into(), DataType::Float64),
            Field::new("bhckg347".into(), DataType::Float64),
            Field::new("bhckg391".into(), DataType::Float64),
            Field::new("bhckg392".into(), DataType::Float64),
            Field::new("bhckg395".into(), DataType::Float64),
            Field::new("bhckg396".into(), DataType::Float64),
            Field::new("bhckg401".into(), DataType::Float64),
            Field::new("bhckg402".into(), DataType::Float64),
            Field::new("bhckg403".into(), DataType::Float64),
            Field::new("bhckg404".into(), DataType::Float64),
            Field::new("bhckg405".into(), DataType::Float64),
            Field::new("bhckg406".into(), DataType::Float64),
            Field::new("bhckg407".into(), DataType::Float64),
            Field::new("bhckg408".into(), DataType::Float64),
            Field::new("bhckg409".into(), DataType::Float64),
            Field::new("bhckg410".into(), DataType::Float64),
            Field::new("bhckg411".into(), DataType::Float64),
            Field::new("bhckg412".into(), DataType::Float64),
            Field::new("bhckg413".into(), DataType::Float64),
            Field::new("bhckg414".into(), DataType::Float64),
            Field::new("bhckg415".into(), DataType::Float64),
            Field::new("bhckg416".into(), DataType::Float64),
            Field::new("bhckg417".into(), DataType::Float64),
            Field::new("bhckg474".into(), DataType::Float64),
            Field::new("bhckg475".into(), DataType::Float64),
            Field::new("bhckg476".into(), DataType::Float64),
            Field::new("bhckg477".into(), DataType::Float64),
            Field::new("bhckg478".into(), DataType::Float64),
            Field::new("bhckg479".into(), DataType::Float64),
            Field::new("bhckg480".into(), DataType::Float64),
            Field::new("bhckg481".into(), DataType::Float64),
            Field::new("bhckg482".into(), DataType::Float64),
            Field::new("bhckg483".into(), DataType::Float64),
            Field::new("bhckg484".into(), DataType::Float64),
            Field::new("bhckg485".into(), DataType::Float64),
            Field::new("bhckg486".into(), DataType::Float64),
            Field::new("bhckg487".into(), DataType::Float64),
            Field::new("bhckg488".into(), DataType::Float64),
            Field::new("bhckg489".into(), DataType::Float64),
            Field::new("bhckg490".into(), DataType::Float64),
            Field::new("bhckg491".into(), DataType::Float64),
            Field::new("bhckg492".into(), DataType::Float64),
            Field::new("bhckg507".into(), DataType::Float64),
            Field::new("bhckg508".into(), DataType::Float64),
            Field::new("bhckg509".into(), DataType::Float64),
            Field::new("bhckg510".into(), DataType::Float64),
            Field::new("bhckg511".into(), DataType::Float64),
            Field::new("bhckg521".into(), DataType::Float64),
            Field::new("bhckg522".into(), DataType::Float64),
            Field::new("bhckg523".into(), DataType::Float64),
            Field::new("bhckg524".into(), DataType::Float64),
            Field::new("bhckg525".into(), DataType::Float64),
            Field::new("bhckg536".into(), DataType::Float64),
            Field::new("bhckg537".into(), DataType::Float64),
            Field::new("bhckg538".into(), DataType::Float64),
            Field::new("bhckg539".into(), DataType::Float64),
            Field::new("bhckg540".into(), DataType::Float64),
            Field::new("bhckg541".into(), DataType::Float64),
            Field::new("bhckg542".into(), DataType::Float64),
            Field::new("bhckg543".into(), DataType::Float64),
            Field::new("bhckg544".into(), DataType::Float64),
            Field::new("bhckg545".into(), DataType::Float64),
            Field::new("bhckg546".into(), DataType::Float64),
            Field::new("bhckg547".into(), DataType::Float64),
            Field::new("bhckg548".into(), DataType::Float64),
            Field::new("bhckg549".into(), DataType::Float64),
            Field::new("bhckg550".into(), DataType::Float64),
            Field::new("bhckg561".into(), DataType::Float64),
            Field::new("bhckg562".into(), DataType::Float64),
            Field::new("bhckg563".into(), DataType::Float64),
            Field::new("bhckg564".into(), DataType::Float64),
            Field::new("bhckg565".into(), DataType::Float64),
            Field::new("bhckg566".into(), DataType::Float64),
            Field::new("bhckg567".into(), DataType::Float64),
            Field::new("bhckg568".into(), DataType::Float64),
            Field::new("bhckg569".into(), DataType::Float64),
            Field::new("bhckg570".into(), DataType::Float64),
            Field::new("bhckg571".into(), DataType::Float64),
            Field::new("bhckg572".into(), DataType::Float64),
            Field::new("bhckg573".into(), DataType::Float64),
            Field::new("bhckg574".into(), DataType::Float64),
            Field::new("bhckg575".into(), DataType::Float64),
            Field::new("bhckg586".into(), DataType::Float64),
            Field::new("bhckg587".into(), DataType::Float64),
            Field::new("bhckg588".into(), DataType::Float64),
            Field::new("bhckg589".into(), DataType::Float64),
            Field::new("bhckg590".into(), DataType::Float64),
            Field::new("bhckg597".into(), DataType::Float64),
            Field::new("bhckg598".into(), DataType::Float64),
            Field::new("bhckg599".into(), DataType::Float64),
            Field::new("bhckg600".into(), DataType::Float64),
            Field::new("bhckg601".into(), DataType::Float64),
            Field::new("bhckg602".into(), DataType::Float64),
            Field::new("bhckg606".into(), DataType::Float64),
            Field::new("bhckg607".into(), DataType::Float64),
            Field::new("bhckg608".into(), DataType::Float64),
            Field::new("bhckg609".into(), DataType::Float64),
            Field::new("bhckg610".into(), DataType::Float64),
            Field::new("bhckg611".into(), DataType::Float64),
            Field::new("bhckg618".into(), DataType::Float64),
            Field::new("bhckg619".into(), DataType::Float64),
            Field::new("bhckg620".into(), DataType::Float64),
            Field::new("bhckg621".into(), DataType::Float64),
            Field::new("bhckg622".into(), DataType::Float64),
            Field::new("bhckg623".into(), DataType::Float64),
            Field::new("bhckg642".into(), DataType::Float64),
            Field::new("bhckg804".into(), DataType::Float64),
            Field::new("bhckg805".into(), DataType::Float64),
            Field::new("bhckg806".into(), DataType::Float64),
            Field::new("bhckg807".into(), DataType::Float64),
            Field::new("bhckg808".into(), DataType::Float64),
            Field::new("bhckg809".into(), DataType::Float64),
            Field::new("bhckg894".into(), DataType::Float64),
            Field::new("bhckg914".into(), DataType::Float64),
            Field::new("bhckh172".into(), DataType::Float64),
            Field::new("bhckh173".into(), DataType::Float64),
            Field::new("bhckh174".into(), DataType::Float64),
            Field::new("bhckh175".into(), DataType::Float64),
            Field::new("bhckh176".into(), DataType::Float64),
            Field::new("bhckh177".into(), DataType::Float64),
            Field::new("bhckh178".into(), DataType::Float64),
            Field::new("bhckh179".into(), DataType::Float64),
            Field::new("bhckh180".into(), DataType::Float64),
            Field::new("bhckh181".into(), DataType::Float64),
            Field::new("bhckh182".into(), DataType::Float64),
            Field::new("bhckh185".into(), DataType::Float64),
            Field::new("bhckh186".into(), DataType::Float64),
            Field::new("bhckh187".into(), DataType::Float64),
            Field::new("bhckh188".into(), DataType::Float64),
            Field::new("bhckh193".into(), DataType::Float64),
            Field::new("bhckh194".into(), DataType::Float64),
            Field::new("bhckh195".into(), DataType::Float64),
            Field::new("bhckh196".into(), DataType::Float64),
            Field::new("bhckh197".into(), DataType::Float64),
            Field::new("bhckh198".into(), DataType::Float64),
            Field::new("bhckh199".into(), DataType::Float64),
            Field::new("bhckh200".into(), DataType::Float64),
            Field::new("bhckh270".into(), DataType::Float64),
            Field::new("bhckh271".into(), DataType::Float64),
            Field::new("bhckh272".into(), DataType::Float64),
            Field::new("bhckh273".into(), DataType::Float64),
            Field::new("bhckh274".into(), DataType::Float64),
            Field::new("bhckh275".into(), DataType::Float64),
            Field::new("bhckh276".into(), DataType::Float64),
            Field::new("bhckh277".into(), DataType::Float64),
            Field::new("bhckh278".into(), DataType::Float64),
            Field::new("bhckh279".into(), DataType::Float64),
            Field::new("bhckh280".into(), DataType::Float64),
            Field::new("bhckh281".into(), DataType::Float64),
            Field::new("bhckh282".into(), DataType::Float64),
            Field::new("bhckh283".into(), DataType::Float64),
            Field::new("bhckh284".into(), DataType::Float64),
            Field::new("bhckh285".into(), DataType::Float64),
            Field::new("bhckh286".into(), DataType::Float64),
            Field::new("bhckh287".into(), DataType::Float64),
            Field::new("bhckh288".into(), DataType::Float64),
            Field::new("bhckh293".into(), DataType::Float64),
            Field::new("bhckh294".into(), DataType::Float64),
            Field::new("bhckh295".into(), DataType::Float64),
            Field::new("bhckh296".into(), DataType::Float64),
            Field::new("bhckh297".into(), DataType::Float64),
            Field::new("bhckh298".into(), DataType::Float64),
            Field::new("bhckh299".into(), DataType::Float64),
            Field::new("bhckhj78".into(), DataType::Float64),
            Field::new("bhckhj79".into(), DataType::Float64),
            Field::new("bhckhj80".into(), DataType::Float64),
            Field::new("bhckhj81".into(), DataType::Float64),
            Field::new("bhckhj82".into(), DataType::Float64),
            Field::new("bhckhj83".into(), DataType::Float64),
            Field::new("bhckhj84".into(), DataType::Float64),
            Field::new("bhckhj85".into(), DataType::Float64),
            Field::new("bhckhj88".into(), DataType::Float64),
            Field::new("bhckhj89".into(), DataType::Float64),
            Field::new("bhckhj92".into(), DataType::Float64),
            Field::new("bhckhj93".into(), DataType::Float64),
            Field::new("bhckhj94".into(), DataType::Float64),
            Field::new("bhckhj95".into(), DataType::Float64),
            Field::new("bhckhk03".into(), DataType::Float64),
            Field::new("bhckhk04".into(), DataType::Float64),
            Field::new("bhckht58".into(), DataType::Float64),
            Field::new("bhckht59".into(), DataType::Float64),
            Field::new("bhckht60".into(), DataType::Float64),
            Field::new("bhckht61".into(), DataType::Float64),
            Field::new("bhckht62".into(), DataType::Float64),
            Field::new("bhckht63".into(), DataType::Float64),
            Field::new("bhckht64".into(), DataType::Float64),
            Field::new("bhckht65".into(), DataType::Float64),
            Field::new("bhckht69".into(), DataType::Float64),
            Field::new("bhckht80".into(), DataType::Float64),
            Field::new("bhckht83".into(), DataType::Float64),
            Field::new("bhckht84".into(), DataType::Float64),
            Field::new("bhckht85".into(), DataType::Float64),
            Field::new("bhckht87".into(), DataType::Float64),
            Field::new("bhckht88".into(), DataType::Float64),
            Field::new("bhckht89".into(), DataType::Float64),
            Field::new("bhckht91".into(), DataType::Float64),
            Field::new("bhckht92".into(), DataType::Float64),
            Field::new("bhckht93".into(), DataType::Float64),
            Field::new("bhckhu09".into(), DataType::Float64),
            Field::new("bhckhu10".into(), DataType::Float64),
            Field::new("bhckhu11".into(), DataType::Float64),
            Field::new("bhckhu12".into(), DataType::Float64),
            Field::new("bhckhu13".into(), DataType::Float64),
            Field::new("bhckhu14".into(), DataType::Float64),
            Field::new("bhckhu15".into(), DataType::Float64),
            Field::new("bhckhu20".into(), DataType::Float64),
            Field::new("bhckhu21".into(), DataType::Float64),
            Field::new("bhckhu22".into(), DataType::Float64),
            Field::new("bhckhu23".into(), DataType::Float64),
            Field::new("bhckj320".into(), DataType::Float64),
            Field::new("bhckj447".into(), DataType::Float64),
            Field::new("bhckj451".into(), DataType::Float64),
            Field::new("bhckj452".into(), DataType::Float64),
            Field::new("bhckj453".into(), DataType::Float64),
            Field::new("bhckj454".into(), DataType::Float64),
            Field::new("bhckj455".into(), DataType::Float64),
            Field::new("bhckj456".into(), DataType::Float64),
            Field::new("bhckj461".into(), DataType::Float64),
            Field::new("bhckj462".into(), DataType::Float64),
            Field::new("bhckj463".into(), DataType::Float64),
            Field::new("bhckj536".into(), DataType::Float64),
            Field::new("bhckj537".into(), DataType::Float64),
            Field::new("bhckj981".into(), DataType::Float64),
            Field::new("bhckj982".into(), DataType::Float64),
            Field::new("bhckj983".into(), DataType::Float64),
            Field::new("bhckj984".into(), DataType::Float64),
            Field::new("bhckj985".into(), DataType::Float64),
            Field::new("bhckj986".into(), DataType::Float64),
            Field::new("bhckj987".into(), DataType::Float64),
            Field::new("bhckj988".into(), DataType::Float64),
            Field::new("bhckj989".into(), DataType::Float64),
            Field::new("bhckj990".into(), DataType::Float64),
            Field::new("bhckj991".into(), DataType::Float64),
            Field::new("bhckj992".into(), DataType::Float64),
            Field::new("bhckj993".into(), DataType::Float64),
            Field::new("bhckj994".into(), DataType::Float64),
            Field::new("bhckj995".into(), DataType::Float64),
            Field::new("bhckj996".into(), DataType::Float64),
            Field::new("bhckj997".into(), DataType::Float64),
            Field::new("bhckj998".into(), DataType::Float64),
            Field::new("bhckj999".into(), DataType::Float64),
            Field::new("bhckja21".into(), DataType::Float64),
            Field::new("bhckja22".into(), DataType::Float64),
            Field::new("bhckjf76".into(), DataType::Float64),
            Field::new("bhckjf84".into(), DataType::Float64),
            Field::new("bhckjf85".into(), DataType::Float64),
            Field::new("bhckjf86".into(), DataType::Float64),
            Field::new("bhckjf87".into(), DataType::Float64),
            Field::new("bhckjf88".into(), DataType::Float64),
            Field::new("bhckjf89".into(), DataType::Float64),
            Field::new("bhckjf90".into(), DataType::Float64),
            Field::new("bhckjf91".into(), DataType::Float64),
            Field::new("bhckjf92".into(), DataType::Float64),
            Field::new("bhckjf93".into(), DataType::Float64),
            Field::new("bhckjh88".into(), DataType::Float64),
            Field::new("bhckjh91".into(), DataType::Float64),
            Field::new("bhckjh92".into(), DataType::Float64),
            Field::new("bhckjh93".into(), DataType::Float64),
            Field::new("bhckjh94".into(), DataType::Float64),
            Field::new("bhckjh97".into(), DataType::Float64),
            Field::new("bhckjh98".into(), DataType::Float64),
            Field::new("bhckjh99".into(), DataType::Float64),
            Field::new("bhckjj00".into(), DataType::Float64),
            Field::new("bhckjj01".into(), DataType::Float64),
            Field::new("bhckjj03".into(), DataType::Float64),
            Field::new("bhckjj04".into(), DataType::Float64),
            Field::new("bhckjj05".into(), DataType::Float64),
            Field::new("bhckjj06".into(), DataType::Float64),
            Field::new("bhckjj07".into(), DataType::Float64),
            Field::new("bhckjj08".into(), DataType::Float64),
            Field::new("bhckjj09".into(), DataType::Float64),
            Field::new("bhckjj11".into(), DataType::Float64),
            Field::new("bhckjj12".into(), DataType::Float64),
            Field::new("bhckjj13".into(), DataType::Float64),
            Field::new("bhckjj14".into(), DataType::Float64),
            Field::new("bhckjj15".into(), DataType::Float64),
            Field::new("bhckjj16".into(), DataType::Float64),
            Field::new("bhckjj17".into(), DataType::Float64),
            Field::new("bhckjj18".into(), DataType::Float64),
            Field::new("bhckjj19".into(), DataType::Float64),
            Field::new("bhckjj20".into(), DataType::Float64),
            Field::new("bhckjj21".into(), DataType::Float64),
            Field::new("bhckjj23".into(), DataType::Float64),
            Field::new("bhckjj24".into(), DataType::Float64),
            Field::new("bhckjj25".into(), DataType::Float64),
            Field::new("bhckjj26".into(), DataType::Float64),
            Field::new("bhckjj27".into(), DataType::Float64),
            Field::new("bhckjj28".into(), DataType::Float64),
            Field::new("bhckjj30".into(), DataType::Float64),
            Field::new("bhckjj31".into(), DataType::Float64),
            Field::new("bhckjj32".into(), DataType::Float64),
            Field::new("bhckjj34".into(), DataType::Float64),
            Field::new("bhckk001".into(), DataType::Float64),
            Field::new("bhckk002".into(), DataType::Float64),
            Field::new("bhckk003".into(), DataType::Float64),
            Field::new("bhckk004".into(), DataType::Float64),
            Field::new("bhckk005".into(), DataType::Float64),
            Field::new("bhckk006".into(), DataType::Float64),
            Field::new("bhckk007".into(), DataType::Float64),
            Field::new("bhckk008".into(), DataType::Float64),
            Field::new("bhckk009".into(), DataType::Float64),
            Field::new("bhckk010".into(), DataType::Float64),
            Field::new("bhckk011".into(), DataType::Float64),
            Field::new("bhckk012".into(), DataType::Float64),
            Field::new("bhckk013".into(), DataType::Float64),
            Field::new("bhckk014".into(), DataType::Float64),
            Field::new("bhckk015".into(), DataType::Float64),
            Field::new("bhckk016".into(), DataType::Float64),
            Field::new("bhckk017".into(), DataType::Float64),
            Field::new("bhckk018".into(), DataType::Float64),
            Field::new("bhckk019".into(), DataType::Float64),
            Field::new("bhckk020".into(), DataType::Float64),
            Field::new("bhckk021".into(), DataType::Float64),
            Field::new("bhckk022".into(), DataType::Float64),
            Field::new("bhckk023".into(), DataType::Float64),
            Field::new("bhckk024".into(), DataType::Float64),
            Field::new("bhckk025".into(), DataType::Float64),
            Field::new("bhckk026".into(), DataType::Float64),
            Field::new("bhckk027".into(), DataType::Float64),
            Field::new("bhckk028".into(), DataType::Float64),
            Field::new("bhckk029".into(), DataType::Float64),
            Field::new("bhckk030".into(), DataType::Float64),
            Field::new("bhckk031".into(), DataType::Float64),
            Field::new("bhckk032".into(), DataType::Float64),
            Field::new("bhckk033".into(), DataType::Float64),
            Field::new("bhckk034".into(), DataType::Float64),
            Field::new("bhckk035".into(), DataType::Float64),
            Field::new("bhckk036".into(), DataType::Float64),
            Field::new("bhckk037".into(), DataType::Float64),
            Field::new("bhckk038".into(), DataType::Float64),
            Field::new("bhckk039".into(), DataType::Float64),
            Field::new("bhckk040".into(), DataType::Float64),
            Field::new("bhckk041".into(), DataType::Float64),
            Field::new("bhckk072".into(), DataType::Float64),
            Field::new("bhckk073".into(), DataType::Float64),
            Field::new("bhckk074".into(), DataType::Float64),
            Field::new("bhckk075".into(), DataType::Float64),
            Field::new("bhckk076".into(), DataType::Float64),
            Field::new("bhckk077".into(), DataType::Float64),
            Field::new("bhckk078".into(), DataType::Float64),
            Field::new("bhckk079".into(), DataType::Float64),
            Field::new("bhckk080".into(), DataType::Float64),
            Field::new("bhckk081".into(), DataType::Float64),
            Field::new("bhckk082".into(), DataType::Float64),
            Field::new("bhckk083".into(), DataType::Float64),
            Field::new("bhckk084".into(), DataType::Float64),
            Field::new("bhckk085".into(), DataType::Float64),
            Field::new("bhckk086".into(), DataType::Float64),
            Field::new("bhckk087".into(), DataType::Float64),
            Field::new("bhckk088".into(), DataType::Float64),
            Field::new("bhckk089".into(), DataType::Float64),
            Field::new("bhckk090".into(), DataType::Float64),
            Field::new("bhckk091".into(), DataType::Float64),
            Field::new("bhckk092".into(), DataType::Float64),
            Field::new("bhckk093".into(), DataType::Float64),
            Field::new("bhckk094".into(), DataType::Float64),
            Field::new("bhckk095".into(), DataType::Float64),
            Field::new("bhckk096".into(), DataType::Float64),
            Field::new("bhckk097".into(), DataType::Float64),
            Field::new("bhckk098".into(), DataType::Float64),
            Field::new("bhckk099".into(), DataType::Float64),
            Field::new("bhckk100".into(), DataType::Float64),
            Field::new("bhckk101".into(), DataType::Float64),
            Field::new("bhckk120".into(), DataType::Float64),
            Field::new("bhckk121".into(), DataType::Float64),
            Field::new("bhckk122".into(), DataType::Float64),
            Field::new("bhckk123".into(), DataType::Float64),
            Field::new("bhckk124".into(), DataType::Float64),
            Field::new("bhckk125".into(), DataType::Float64),
            Field::new("bhckk126".into(), DataType::Float64),
            Field::new("bhckk127".into(), DataType::Float64),
            Field::new("bhckk128".into(), DataType::Float64),
            Field::new("bhckk129".into(), DataType::Float64),
            Field::new("bhckk134".into(), DataType::Float64),
            Field::new("bhckk135".into(), DataType::Float64),
            Field::new("bhckk136".into(), DataType::Float64),
            Field::new("bhckk137".into(), DataType::Float64),
            Field::new("bhckk138".into(), DataType::Float64),
            Field::new("bhckk139".into(), DataType::Float64),
            Field::new("bhckk140".into(), DataType::Float64),
            Field::new("bhckk142".into(), DataType::Float64),
            Field::new("bhckk143".into(), DataType::Float64),
            Field::new("bhckk144".into(), DataType::Float64),
            Field::new("bhckk145".into(), DataType::Float64),
            Field::new("bhckk146".into(), DataType::Float64),
            Field::new("bhckk147".into(), DataType::Float64),
            Field::new("bhckk148".into(), DataType::Float64),
            Field::new("bhckk149".into(), DataType::Float64),
            Field::new("bhckk150".into(), DataType::Float64),
            Field::new("bhckk151".into(), DataType::Float64),
            Field::new("bhckk152".into(), DataType::Float64),
            Field::new("bhckk153".into(), DataType::Float64),
            Field::new("bhckk154".into(), DataType::Float64),
            Field::new("bhckk155".into(), DataType::Float64),
            Field::new("bhckk156".into(), DataType::Float64),
            Field::new("bhckk157".into(), DataType::Float64),
            Field::new("bhckk163".into(), DataType::Float64),
            Field::new("bhckk164".into(), DataType::Float64),
            Field::new("bhckk165".into(), DataType::Float64),
            Field::new("bhckk167".into(), DataType::Float64),
            Field::new("bhckk168".into(), DataType::Float64),
            Field::new("bhckk178".into(), DataType::Float64),
            Field::new("bhckk179".into(), DataType::Float64),
            Field::new("bhckk180".into(), DataType::Float64),
            Field::new("bhckk181".into(), DataType::Float64),
            Field::new("bhckk182".into(), DataType::Float64),
            Field::new("bhckk183".into(), DataType::Float64),
            Field::new("bhckk184".into(), DataType::Float64),
            Field::new("bhckk185".into(), DataType::Float64),
            Field::new("bhckk186".into(), DataType::Float64),
            Field::new("bhckk192".into(), DataType::Float64),
            Field::new("bhckk193".into(), DataType::Float64),
            Field::new("bhckk194".into(), DataType::Float64),
            Field::new("bhckk196".into(), DataType::Float64),
            Field::new("bhckk201".into(), DataType::Float64),
            Field::new("bhckk202".into(), DataType::Float64),
            Field::new("bhckk203".into(), DataType::Float64),
            Field::new("bhckk204".into(), DataType::Float64),
            Field::new("bhckk205".into(), DataType::Float64),
            Field::new("bhckk207".into(), DataType::Float64),
            Field::new("bhckk208".into(), DataType::Float64),
            Field::new("bhckk212".into(), DataType::Float64),
            Field::new("bhckk213".into(), DataType::Float64),
            Field::new("bhckk214".into(), DataType::Float64),
            Field::new("bhckk215".into(), DataType::Float64),
            Field::new("bhckk216".into(), DataType::Float64),
            Field::new("bhckk217".into(), DataType::Float64),
            Field::new("bhckk218".into(), DataType::Float64),
            Field::new("bhckk267".into(), DataType::Float64),
            Field::new("bhckk269".into(), DataType::Float64),
            Field::new("bhckk270".into(), DataType::Float64),
            Field::new("bhckk271".into(), DataType::Float64),
            Field::new("bhckk272".into(), DataType::Float64),
            Field::new("bhckk273".into(), DataType::Float64),
            Field::new("bhckk274".into(), DataType::Float64),
            Field::new("bhckk275".into(), DataType::Float64),
            Field::new("bhckk276".into(), DataType::Float64),
            Field::new("bhckk277".into(), DataType::Float64),
            Field::new("bhckk278".into(), DataType::Float64),
            Field::new("bhckk279".into(), DataType::Float64),
            Field::new("bhckk280".into(), DataType::Float64),
            Field::new("bhckk281".into(), DataType::Float64),
            Field::new("bhckk282".into(), DataType::Float64),
            Field::new("bhckk283".into(), DataType::Float64),
            Field::new("bhckk284".into(), DataType::Float64),
            Field::new("bhckk285".into(), DataType::Float64),
            Field::new("bhckk286".into(), DataType::Float64),
            Field::new("bhckk287".into(), DataType::Float64),
            Field::new("bhckk288".into(), DataType::Float64),
            Field::new("bhckkx46".into(), DataType::Float64),
            Field::new("bhckkx47".into(), DataType::Float64),
            Field::new("bhckkx50".into(), DataType::Float64),
            Field::new("bhckkx51".into(), DataType::Float64),
            Field::new("bhckkx52".into(), DataType::Float64),
            Field::new("bhckkx53".into(), DataType::Float64),
            Field::new("bhckkx54".into(), DataType::Float64),
            Field::new("bhckkx55".into(), DataType::Float64),
            Field::new("bhckkx57".into(), DataType::Float64),
            Field::new("bhckkx58".into(), DataType::Float64),
            Field::new("bhckkx60".into(), DataType::Float64),
            Field::new("bhckkx61".into(), DataType::Float64),
            Field::new("bhckkx62".into(), DataType::Float64),
            Field::new("bhckkx63".into(), DataType::Float64),
            Field::new("bhckkx64".into(), DataType::Float64),
            Field::new("bhckkx65".into(), DataType::Float64),
            Field::new("bhckky38".into(), DataType::Boolean),
            Field::new("bhcklg24".into(), DataType::Boolean),
            Field::new("bhcklg26".into(), DataType::Float64),
            Field::new("bhckm727".into(), DataType::Float64),
            Field::new("bhckm728".into(), DataType::Float64),
            Field::new("bhckm729".into(), DataType::Float64),
            Field::new("bhckm730".into(), DataType::Float64),
            Field::new("bhckm731".into(), DataType::Float64),
            Field::new("bhckm732".into(), DataType::Float64),
            Field::new("bhckm733".into(), DataType::Float64),
            Field::new("bhckm734".into(), DataType::Float64),
            Field::new("bhckm735".into(), DataType::Float64),
            Field::new("bhckm736".into(), DataType::Float64),
            Field::new("bhckm737".into(), DataType::Float64),
            Field::new("bhckm738".into(), DataType::Float64),
            Field::new("bhckm739".into(), DataType::Float64),
            Field::new("bhckm740".into(), DataType::Float64),
            Field::new("bhckm741".into(), DataType::Float64),
            Field::new("bhckm742".into(), DataType::Float64),
            Field::new("bhckm743".into(), DataType::Float64),
            Field::new("bhckm744".into(), DataType::Float64),
            Field::new("bhckm962".into(), DataType::Float64),
            Field::new("bhckmg94".into(), DataType::Float64),
            Field::new("bhcks396".into(), DataType::Float64),
            Field::new("bhcks397".into(), DataType::Float64),
            Field::new("bhcks398".into(), DataType::Float64),
            Field::new("bhcks399".into(), DataType::Float64),
            Field::new("bhcks400".into(), DataType::Float64),
            Field::new("bhcks402".into(), DataType::Float64),
            Field::new("bhcks403".into(), DataType::Float64),
            Field::new("bhcks405".into(), DataType::Float64),
            Field::new("bhcks406".into(), DataType::Float64),
            Field::new("bhcks410".into(), DataType::Float64),
            Field::new("bhcks411".into(), DataType::Float64),
            Field::new("bhcks414".into(), DataType::Float64),
            Field::new("bhcks415".into(), DataType::Float64),
            Field::new("bhcks416".into(), DataType::Float64),
            Field::new("bhcks417".into(), DataType::Float64),
            Field::new("bhcks420".into(), DataType::Float64),
            Field::new("bhcks421".into(), DataType::Float64),
            Field::new("bhcks424".into(), DataType::Float64),
            Field::new("bhcks425".into(), DataType::Float64),
            Field::new("bhcks426".into(), DataType::Float64),
            Field::new("bhcks427".into(), DataType::Float64),
            Field::new("bhcks428".into(), DataType::Float64),
            Field::new("bhcks429".into(), DataType::Float64),
            Field::new("bhcks432".into(), DataType::Float64),
            Field::new("bhcks433".into(), DataType::Float64),
            Field::new("bhcks434".into(), DataType::Float64),
            Field::new("bhcks435".into(), DataType::Float64),
            Field::new("bhcks436".into(), DataType::Float64),
            Field::new("bhcks437".into(), DataType::Float64),
            Field::new("bhcks440".into(), DataType::Float64),
            Field::new("bhcks441".into(), DataType::Float64),
            Field::new("bhcks442".into(), DataType::Float64),
            Field::new("bhcks443".into(), DataType::Float64),
            Field::new("bhcks446".into(), DataType::Float64),
            Field::new("bhcks447".into(), DataType::Float64),
            Field::new("bhcks450".into(), DataType::Float64),
            Field::new("bhcks451".into(), DataType::Float64),
            Field::new("bhcks452".into(), DataType::Float64),
            Field::new("bhcks453".into(), DataType::Float64),
            Field::new("bhcks454".into(), DataType::Float64),
            Field::new("bhcks455".into(), DataType::Float64),
            Field::new("bhcks458".into(), DataType::Float64),
            Field::new("bhcks459".into(), DataType::Float64),
            Field::new("bhcks460".into(), DataType::Float64),
            Field::new("bhcks461".into(), DataType::Float64),
            Field::new("bhcks462".into(), DataType::Float64),
            Field::new("bhcks463".into(), DataType::Float64),
            Field::new("bhcks469".into(), DataType::Float64),
            Field::new("bhcks470".into(), DataType::Float64),
            Field::new("bhcks471".into(), DataType::Float64),
            Field::new("bhcks476".into(), DataType::Float64),
            Field::new("bhcks477".into(), DataType::Float64),
            Field::new("bhcks478".into(), DataType::Float64),
            Field::new("bhcks479".into(), DataType::Float64),
            Field::new("bhcks481".into(), DataType::Float64),
            Field::new("bhcks482".into(), DataType::Float64),
            Field::new("bhcks483".into(), DataType::Float64),
            Field::new("bhcks484".into(), DataType::Float64),
            Field::new("bhcks486".into(), DataType::Float64),
            Field::new("bhcks487".into(), DataType::Float64),
            Field::new("bhcks488".into(), DataType::Float64),
            Field::new("bhcks489".into(), DataType::Float64),
            Field::new("bhcks491".into(), DataType::Float64),
            Field::new("bhcks492".into(), DataType::Float64),
            Field::new("bhcks493".into(), DataType::Float64),
            Field::new("bhcks494".into(), DataType::Float64),
            Field::new("bhcks496".into(), DataType::Float64),
            Field::new("bhcks497".into(), DataType::Float64),
            Field::new("bhcks498".into(), DataType::Float64),
            Field::new("bhcks499".into(), DataType::Float64),
            Field::new("bhcks511".into(), DataType::Float64),
            Field::new("bhcks513".into(), DataType::Float64),
            Field::new("bhcks524".into(), DataType::Float64),
            Field::new("bhcks549".into(), DataType::Float64),
            Field::new("bhcks550".into(), DataType::Float64),
            Field::new("bhcks551".into(), DataType::Float64),
            Field::new("bhcks552".into(), DataType::Float64),
            Field::new("bhcks554".into(), DataType::Float64),
            Field::new("bhcks555".into(), DataType::Float64),
            Field::new("bhcks556".into(), DataType::Float64),
            Field::new("bhcks557".into(), DataType::Float64),
            Field::new("bhcks582".into(), DataType::Float64),
            Field::new("bhcks583".into(), DataType::Float64),
            Field::new("bhcks584".into(), DataType::Float64),
            Field::new("bhcks585".into(), DataType::Float64),
            Field::new("bhcks586".into(), DataType::Float64),
            Field::new("bhcks587".into(), DataType::Float64),
            Field::new("bhcks588".into(), DataType::Float64),
            Field::new("bhcks589".into(), DataType::Float64),
            Field::new("bhcks590".into(), DataType::Float64),
            Field::new("bhcks591".into(), DataType::Float64),
            Field::new("bhcks592".into(), DataType::Float64),
            Field::new("bhcks593".into(), DataType::Float64),
            Field::new("bhcks594".into(), DataType::Float64),
            Field::new("bhcks595".into(), DataType::Float64),
            Field::new("bhcks596".into(), DataType::Float64),
            Field::new("bhcks597".into(), DataType::Float64),
            Field::new("bhcks598".into(), DataType::Float64),
            Field::new("bhcks599".into(), DataType::Float64),
            Field::new("bhcks600".into(), DataType::Float64),
            Field::new("bhcks601".into(), DataType::Float64),
            Field::new("bhcks602".into(), DataType::Float64),
            Field::new("bhcks603".into(), DataType::Float64),
            Field::new("bhcks604".into(), DataType::Float64),
            Field::new("bhcks605".into(), DataType::Float64),
            Field::new("bhcks606".into(), DataType::Float64),
            Field::new("bhcks607".into(), DataType::Float64),
            Field::new("bhcks608".into(), DataType::Float64),
            Field::new("bhcks609".into(), DataType::Float64),
            Field::new("bhcks610".into(), DataType::Float64),
            Field::new("bhcks611".into(), DataType::Float64),
            Field::new("bhcks612".into(), DataType::Float64),
            Field::new("bhcks613".into(), DataType::Float64),
            Field::new("bhcks614".into(), DataType::Float64),
            Field::new("bhcks615".into(), DataType::Float64),
            Field::new("bhcks616".into(), DataType::Float64),
            Field::new("bhcks617".into(), DataType::Float64),
            Field::new("bhcks618".into(), DataType::Float64),
            Field::new("bhcks619".into(), DataType::Float64),
            Field::new("bhcks620".into(), DataType::Float64),
            Field::new("bhcks621".into(), DataType::Float64),
            Field::new("bhcks622".into(), DataType::Float64),
            Field::new("bhcks623".into(), DataType::Float64),
            Field::new("bhckt047".into(), DataType::Float64),
            Field::new("bhcky923".into(), DataType::Float64),
            Field::new("bhcky924".into(), DataType::Float64),
            Field::new("rssd9001".into(), DataType::Float64),
            Field::new("rssd9017".into(), DataType::String),
            Field::new("rssd9999".into(), DataType::Float64), // DataType::Date),
            Field::new("wrdsdownloaddate".into(), DataType::Date),
        ])
    }

    pub async fn duck_from_parquet(
        conn: Arc<Mutex<Connection>>,
        parquet_path: impl AsRef<Path>,
    ) -> Result<usize, AppError> {
        <Self as DuckCrudModel>::upsert_from_parquet_one_file(
            conn,
            parquet_path,
            None,
            Some(<Self as DuckCrudModel>::table().into()),
        )
        .await
    }

    pub async fn read_range_to_parquet(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
        out_path: impl AsRef<Path>,
    ) -> Result<std::path::PathBuf, AppError> {
        let out_path = out_path.as_ref().to_path_buf();
        tokio::task::spawn_blocking(move || {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if out_path.exists() {
                std::fs::remove_file(&out_path)?;
            }

            let table = <Self as DuckCrudModel>::table();
            let out_sql = out_path.to_string_lossy().replace('\'', "''");
            let sql = format!(
                r#"COPY (
    SELECT * REPLACE (
        TRY_CAST(rssd9001 AS DOUBLE) AS rssd9001,
        TRY_CAST(strftime(CAST(rssd9999 AS DATE), '%Y%m%d') AS DOUBLE) AS rssd9999
    )
    FROM {table}
    WHERE CAST(rssd9999 AS DATE) BETWEEN DATE '{start}' AND DATE '{end}'
) TO '{out}' (FORMAT PARQUET);"#,
                table = table,
                start = date_range.0.to_string(),
                end = date_range.1.to_string(),
                out = out_sql
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            conn_guard.execute_batch(&sql)?;
            Ok::<std::path::PathBuf, AppError>(out_path)
        })
        .await?
    }

    pub async fn read_range<'a>(
        conn: Arc<Mutex<Connection>>,
        date_range: (NaiveDate, NaiveDate),
    ) -> Result<Vec<Row<'a>>, AppError> {
        tokio::task::spawn_blocking(move || {
            let table = <Self as DuckCrudModel>::table();
            let sql = format!(
                r#"SELECT
    CAST(bhck0010 AS DOUBLE) AS bhck0010,
    CAST(bhck0081 AS DOUBLE) AS bhck0081,
    CAST(bhck0211 AS DOUBLE) AS bhck0211,
    CAST(bhck0213 AS DOUBLE) AS bhck0213,
    CAST(bhck0379 AS DOUBLE) AS bhck0379,
    CAST(bhck0395 AS DOUBLE) AS bhck0395,
    CAST(bhck0397 AS DOUBLE) AS bhck0397,
    CAST(bhck0426 AS DOUBLE) AS bhck0426,
    CAST(bhck0497 AS DOUBLE) AS bhck0497,
    CAST(bhck1226 AS DOUBLE) AS bhck1226,
    CAST(bhck1227 AS DOUBLE) AS bhck1227,
    CAST(bhck1228 AS DOUBLE) AS bhck1228,
    CAST(bhck1286 AS DOUBLE) AS bhck1286,
    CAST(bhck1287 AS DOUBLE) AS bhck1287,
    CAST(bhck1288 AS DOUBLE) AS bhck1288,
    CAST(bhck1289 AS DOUBLE) AS bhck1289,
    CAST(bhck1290 AS DOUBLE) AS bhck1290,
    CAST(bhck1291 AS DOUBLE) AS bhck1291,
    CAST(bhck1292 AS DOUBLE) AS bhck1292,
    CAST(bhck1293 AS DOUBLE) AS bhck1293,
    CAST(bhck1294 AS DOUBLE) AS bhck1294,
    CAST(bhck1295 AS DOUBLE) AS bhck1295,
    CAST(bhck1296 AS DOUBLE) AS bhck1296,
    CAST(bhck1297 AS DOUBLE) AS bhck1297,
    CAST(bhck1298 AS DOUBLE) AS bhck1298,
    CAST(bhck1350 AS DOUBLE) AS bhck1350,
    CAST(bhck1410 AS DOUBLE) AS bhck1410,
    CAST(bhck1421 AS BOOLEAN) AS bhck1421,
    CAST(bhck1422 AS DOUBLE) AS bhck1422,
    CAST(bhck1423 AS DOUBLE) AS bhck1423,
    CAST(bhck1545 AS DOUBLE) AS bhck1545,
    CAST(bhck1563 AS DOUBLE) AS bhck1563,
    CAST(bhck1564 AS DOUBLE) AS bhck1564,
    CAST(bhck1583 AS DOUBLE) AS bhck1583,
    CAST(bhck1590 AS DOUBLE) AS bhck1590,
    CAST(bhck1594 AS DOUBLE) AS bhck1594,
    CAST(bhck1597 AS DOUBLE) AS bhck1597,
    CAST(bhck1606 AS DOUBLE) AS bhck1606,
    CAST(bhck1607 AS DOUBLE) AS bhck1607,
    CAST(bhck1608 AS DOUBLE) AS bhck1608,
    CAST(bhck1611 AS DOUBLE) AS bhck1611,
    CAST(bhck1612 AS DOUBLE) AS bhck1612,
    CAST(bhck1613 AS DOUBLE) AS bhck1613,
    CAST(bhck1615 AS DOUBLE) AS bhck1615,
    CAST(bhck1616 AS DOUBLE) AS bhck1616,
    CAST(bhck1635 AS DOUBLE) AS bhck1635,
    CAST(bhck1636 AS DOUBLE) AS bhck1636,
    CAST(bhck1638 AS DOUBLE) AS bhck1638,
    CAST(bhck1639 AS DOUBLE) AS bhck1639,
    CAST(bhck1651 AS DOUBLE) AS bhck1651,
    CAST(bhck1698 AS DOUBLE) AS bhck1698,
    CAST(bhck1699 AS DOUBLE) AS bhck1699,
    CAST(bhck1701 AS DOUBLE) AS bhck1701,
    CAST(bhck1702 AS DOUBLE) AS bhck1702,
    CAST(bhck1703 AS DOUBLE) AS bhck1703,
    CAST(bhck1705 AS DOUBLE) AS bhck1705,
    CAST(bhck1706 AS DOUBLE) AS bhck1706,
    CAST(bhck1707 AS DOUBLE) AS bhck1707,
    CAST(bhck1709 AS DOUBLE) AS bhck1709,
    CAST(bhck1710 AS DOUBLE) AS bhck1710,
    CAST(bhck1711 AS DOUBLE) AS bhck1711,
    CAST(bhck1713 AS DOUBLE) AS bhck1713,
    CAST(bhck1714 AS DOUBLE) AS bhck1714,
    CAST(bhck1715 AS DOUBLE) AS bhck1715,
    CAST(bhck1716 AS DOUBLE) AS bhck1716,
    CAST(bhck1717 AS DOUBLE) AS bhck1717,
    CAST(bhck1718 AS DOUBLE) AS bhck1718,
    CAST(bhck1719 AS DOUBLE) AS bhck1719,
    CAST(bhck1727 AS DOUBLE) AS bhck1727,
    CAST(bhck1731 AS DOUBLE) AS bhck1731,
    CAST(bhck1732 AS DOUBLE) AS bhck1732,
    CAST(bhck1733 AS DOUBLE) AS bhck1733,
    CAST(bhck1734 AS DOUBLE) AS bhck1734,
    CAST(bhck1735 AS DOUBLE) AS bhck1735,
    CAST(bhck1736 AS DOUBLE) AS bhck1736,
    CAST(bhck1737 AS DOUBLE) AS bhck1737,
    CAST(bhck1738 AS DOUBLE) AS bhck1738,
    CAST(bhck1739 AS DOUBLE) AS bhck1739,
    CAST(bhck1741 AS DOUBLE) AS bhck1741,
    CAST(bhck1742 AS DOUBLE) AS bhck1742,
    CAST(bhck1743 AS DOUBLE) AS bhck1743,
    CAST(bhck1744 AS DOUBLE) AS bhck1744,
    CAST(bhck1746 AS DOUBLE) AS bhck1746,
    CAST(bhck1752 AS DOUBLE) AS bhck1752,
    CAST(bhck1753 AS DOUBLE) AS bhck1753,
    CAST(bhck1754 AS DOUBLE) AS bhck1754,
    CAST(bhck1755 AS DOUBLE) AS bhck1755,
    CAST(bhck1763 AS DOUBLE) AS bhck1763,
    CAST(bhck1764 AS DOUBLE) AS bhck1764,
    CAST(bhck1766 AS DOUBLE) AS bhck1766,
    CAST(bhck1773 AS DOUBLE) AS bhck1773,
    CAST(bhck1778 AS DOUBLE) AS bhck1778,
    CAST(bhck1912 AS DOUBLE) AS bhck1912,
    CAST(bhck1913 AS DOUBLE) AS bhck1913,
    CAST(bhck1975 AS DOUBLE) AS bhck1975,
    CAST(bhck2008 AS DOUBLE) AS bhck2008,
    CAST(bhck2011 AS DOUBLE) AS bhck2011,
    CAST(bhck2081 AS DOUBLE) AS bhck2081,
    CAST(bhck2130 AS DOUBLE) AS bhck2130,
    CAST(bhck2143 AS DOUBLE) AS bhck2143,
    CAST(bhck2148 AS DOUBLE) AS bhck2148,
    CAST(bhck2150 AS DOUBLE) AS bhck2150,
    CAST(bhck2155 AS DOUBLE) AS bhck2155,
    CAST(bhck2160 AS DOUBLE) AS bhck2160,
    CAST(bhck2165 AS DOUBLE) AS bhck2165,
    CAST(bhck2168 AS DOUBLE) AS bhck2168,
    CAST(bhck2182 AS DOUBLE) AS bhck2182,
    CAST(bhck2183 AS DOUBLE) AS bhck2183,
    CAST(bhck2309 AS DOUBLE) AS bhck2309,
    CAST(bhck2332 AS DOUBLE) AS bhck2332,
    CAST(bhck2333 AS DOUBLE) AS bhck2333,
    CAST(bhck2745 AS DOUBLE) AS bhck2745,
    CAST(bhck2746 AS DOUBLE) AS bhck2746,
    CAST(bhck2747 AS DOUBLE) AS bhck2747,
    CAST(bhck2748 AS DOUBLE) AS bhck2748,
    CAST(bhck2749 AS DOUBLE) AS bhck2749,
    CAST(bhck2750 AS DOUBLE) AS bhck2750,
    CAST(bhck2757 AS DOUBLE) AS bhck2757,
    CAST(bhck2759 AS DOUBLE) AS bhck2759,
    CAST(bhck2769 AS DOUBLE) AS bhck2769,
    CAST(bhck2771 AS DOUBLE) AS bhck2771,
    CAST(bhck2800 AS DOUBLE) AS bhck2800,
    CAST(bhck2920 AS DOUBLE) AS bhck2920,
    CAST(bhck3000 AS DOUBLE) AS bhck3000,
    CAST(bhck3049 AS DOUBLE) AS bhck3049,
    CAST(bhck3123 AS DOUBLE) AS bhck3123,
    CAST(bhck3124 AS DOUBLE) AS bhck3124,
    CAST(bhck3128 AS DOUBLE) AS bhck3128,
    CAST(bhck3153 AS DOUBLE) AS bhck3153,
    CAST(bhck3163 AS DOUBLE) AS bhck3163,
    CAST(bhck3164 AS DOUBLE) AS bhck3164,
    CAST(bhck3190 AS DOUBLE) AS bhck3190,
    CAST(bhck3197 AS DOUBLE) AS bhck3197,
    CAST(bhck3215 AS DOUBLE) AS bhck3215,
    CAST(bhck3216 AS DOUBLE) AS bhck3216,
    CAST(bhck3217 AS DOUBLE) AS bhck3217,
    CAST(bhck3230 AS DOUBLE) AS bhck3230,
    CAST(bhck3284 AS DOUBLE) AS bhck3284,
    CAST(bhck3296 AS DOUBLE) AS bhck3296,
    CAST(bhck3297 AS DOUBLE) AS bhck3297,
    CAST(bhck3298 AS DOUBLE) AS bhck3298,
    CAST(bhck3409 AS DOUBLE) AS bhck3409,
    CAST(bhck3411 AS DOUBLE) AS bhck3411,
    CAST(bhck3430 AS DOUBLE) AS bhck3430,
    CAST(bhck3434 AS DOUBLE) AS bhck3434,
    CAST(bhck3435 AS DOUBLE) AS bhck3435,
    CAST(bhck3450 AS DOUBLE) AS bhck3450,
    CAST(bhck3451 AS BOOLEAN) AS bhck3451,
    CAST(bhck3452 AS BOOLEAN) AS bhck3452,
    CAST(bhck3492 AS DOUBLE) AS bhck3492,
    CAST(bhck3493 AS DOUBLE) AS bhck3493,
    CAST(bhck3494 AS DOUBLE) AS bhck3494,
    CAST(bhck3495 AS DOUBLE) AS bhck3495,
    CAST(bhck3499 AS DOUBLE) AS bhck3499,
    CAST(bhck3500 AS DOUBLE) AS bhck3500,
    CAST(bhck3501 AS DOUBLE) AS bhck3501,
    CAST(bhck3502 AS DOUBLE) AS bhck3502,
    CAST(bhck3503 AS DOUBLE) AS bhck3503,
    CAST(bhck3504 AS DOUBLE) AS bhck3504,
    CAST(bhck3505 AS DOUBLE) AS bhck3505,
    CAST(bhck3506 AS DOUBLE) AS bhck3506,
    CAST(bhck3507 AS DOUBLE) AS bhck3507,
    CAST(bhck3508 AS DOUBLE) AS bhck3508,
    CAST(bhck3522 AS BOOLEAN) AS bhck3522,
    CAST(bhck3528 AS DOUBLE) AS bhck3528,
    CAST(bhck3529 AS DOUBLE) AS bhck3529,
    CAST(bhck3530 AS DOUBLE) AS bhck3530,
    CAST(bhck3541 AS DOUBLE) AS bhck3541,
    CAST(bhck3546 AS DOUBLE) AS bhck3546,
    CAST(bhck3571 AS DOUBLE) AS bhck3571,
    CAST(bhck3572 AS DOUBLE) AS bhck3572,
    CAST(bhck3574 AS DOUBLE) AS bhck3574,
    CAST(bhck3576 AS DOUBLE) AS bhck3576,
    CAST(bhck3578 AS DOUBLE) AS bhck3578,
    CAST(bhck3580 AS DOUBLE) AS bhck3580,
    CAST(bhck3581 AS DOUBLE) AS bhck3581,
    CAST(bhck3582 AS DOUBLE) AS bhck3582,
    CAST(bhck3584 AS DOUBLE) AS bhck3584,
    CAST(bhck3588 AS DOUBLE) AS bhck3588,
    CAST(bhck3590 AS DOUBLE) AS bhck3590,
    CAST(bhck3656 AS DOUBLE) AS bhck3656,
    CAST(bhck3806 AS DOUBLE) AS bhck3806,
    CAST(bhck3809 AS DOUBLE) AS bhck3809,
    CAST(bhck3812 AS DOUBLE) AS bhck3812,
    CAST(bhck3816 AS DOUBLE) AS bhck3816,
    CAST(bhck3820 AS DOUBLE) AS bhck3820,
    CAST(bhck3822 AS DOUBLE) AS bhck3822,
    CAST(bhck3826 AS DOUBLE) AS bhck3826,
    CAST(bhck3836 AS DOUBLE) AS bhck3836,
    CAST(bhck3837 AS DOUBLE) AS bhck3837,
    CAST(bhck4010 AS DOUBLE) AS bhck4010,
    CAST(bhck4020 AS DOUBLE) AS bhck4020,
    CAST(bhck4027 AS DOUBLE) AS bhck4027,
    CAST(bhck4042 AS DOUBLE) AS bhck4042,
    CAST(bhck4059 AS DOUBLE) AS bhck4059,
    CAST(bhck4060 AS DOUBLE) AS bhck4060,
    CAST(bhck4065 AS DOUBLE) AS bhck4065,
    CAST(bhck4069 AS DOUBLE) AS bhck4069,
    CAST(bhck4070 AS DOUBLE) AS bhck4070,
    CAST(bhck4074 AS DOUBLE) AS bhck4074,
    CAST(bhck4078 AS DOUBLE) AS bhck4078,
    CAST(bhck4092 AS DOUBLE) AS bhck4092,
    CAST(bhck4105 AS DOUBLE) AS bhck4105,
    CAST(bhck4106 AS DOUBLE) AS bhck4106,
    CAST(bhck4115 AS DOUBLE) AS bhck4115,
    CAST(bhck4136 AS DOUBLE) AS bhck4136,
    CAST(bhck4141 AS DOUBLE) AS bhck4141,
    CAST(bhck4146 AS DOUBLE) AS bhck4146,
    CAST(bhck4150 AS DOUBLE) AS bhck4150,
    CAST(bhck4172 AS DOUBLE) AS bhck4172,
    CAST(bhck4180 AS DOUBLE) AS bhck4180,
    CAST(bhck4185 AS DOUBLE) AS bhck4185,
    CAST(bhck4217 AS DOUBLE) AS bhck4217,
    CAST(bhck4219 AS DOUBLE) AS bhck4219,
    CAST(bhck4300 AS DOUBLE) AS bhck4300,
    CAST(bhck4301 AS DOUBLE) AS bhck4301,
    CAST(bhck4302 AS DOUBLE) AS bhck4302,
    CAST(bhck4313 AS DOUBLE) AS bhck4313,
    CAST(bhck4320 AS DOUBLE) AS bhck4320,
    CAST(bhck4336 AS DOUBLE) AS bhck4336,
    CAST(bhck4340 AS DOUBLE) AS bhck4340,
    CAST(bhck4356 AS DOUBLE) AS bhck4356,
    CAST(bhck4393 AS DOUBLE) AS bhck4393,
    CAST(bhck4394 AS DOUBLE) AS bhck4394,
    CAST(bhck4395 AS DOUBLE) AS bhck4395,
    CAST(bhck4396 AS DOUBLE) AS bhck4396,
    CAST(bhck4397 AS DOUBLE) AS bhck4397,
    CAST(bhck4398 AS DOUBLE) AS bhck4398,
    CAST(bhck4399 AS DOUBLE) AS bhck4399,
    CAST(bhck4411 AS DOUBLE) AS bhck4411,
    CAST(bhck4412 AS DOUBLE) AS bhck4412,
    CAST(bhck4414 AS DOUBLE) AS bhck4414,
    CAST(bhck4435 AS DOUBLE) AS bhck4435,
    CAST(bhck4436 AS DOUBLE) AS bhck4436,
    CAST(bhck4460 AS DOUBLE) AS bhck4460,
    CAST(bhck4484 AS DOUBLE) AS bhck4484,
    CAST(bhck4503 AS DOUBLE) AS bhck4503,
    CAST(bhck4504 AS DOUBLE) AS bhck4504,
    CAST(bhck4506 AS DOUBLE) AS bhck4506,
    CAST(bhck4507 AS DOUBLE) AS bhck4507,
    CAST(bhck4518 AS DOUBLE) AS bhck4518,
    CAST(bhck4519 AS DOUBLE) AS bhck4519,
    CAST(bhck4531 AS DOUBLE) AS bhck4531,
    CAST(bhck4574 AS DOUBLE) AS bhck4574,
    CAST(bhck4591 AS DOUBLE) AS bhck4591,
    CAST(bhck4592 AS DOUBLE) AS bhck4592,
    CAST(bhck4598 AS DOUBLE) AS bhck4598,
    CAST(bhck4635 AS DOUBLE) AS bhck4635,
    CAST(bhck4643 AS DOUBLE) AS bhck4643,
    CAST(bhck4644 AS DOUBLE) AS bhck4644,
    CAST(bhck4645 AS DOUBLE) AS bhck4645,
    CAST(bhck4646 AS DOUBLE) AS bhck4646,
    CAST(bhck4651 AS DOUBLE) AS bhck4651,
    CAST(bhck4652 AS DOUBLE) AS bhck4652,
    CAST(bhck4653 AS DOUBLE) AS bhck4653,
    CAST(bhck4654 AS DOUBLE) AS bhck4654,
    CAST(bhck4655 AS DOUBLE) AS bhck4655,
    CAST(bhck4656 AS DOUBLE) AS bhck4656,
    CAST(bhck4657 AS DOUBLE) AS bhck4657,
    CAST(bhck4658 AS DOUBLE) AS bhck4658,
    CAST(bhck4659 AS DOUBLE) AS bhck4659,
    CAST(bhck4776 AS DOUBLE) AS bhck4776,
    CAST(bhck4815 AS DOUBLE) AS bhck4815,
    CAST(bhck4832 AS DOUBLE) AS bhck4832,
    CAST(bhck4833 AS DOUBLE) AS bhck4833,
    CAST(bhck4834 AS DOUBLE) AS bhck4834,
    CAST(bhck5041 AS DOUBLE) AS bhck5041,
    CAST(bhck5043 AS DOUBLE) AS bhck5043,
    CAST(bhck5045 AS DOUBLE) AS bhck5045,
    CAST(bhck5047 AS DOUBLE) AS bhck5047,
    CAST(bhck5310 AS DOUBLE) AS bhck5310,
    CAST(bhck5351 AS DOUBLE) AS bhck5351,
    CAST(bhck5354 AS DOUBLE) AS bhck5354,
    CAST(bhck5355 AS DOUBLE) AS bhck5355,
    CAST(bhck5356 AS DOUBLE) AS bhck5356,
    CAST(bhck5359 AS DOUBLE) AS bhck5359,
    CAST(bhck5360 AS DOUBLE) AS bhck5360,
    CAST(bhck5369 AS DOUBLE) AS bhck5369,
    CAST(bhck5377 AS DOUBLE) AS bhck5377,
    CAST(bhck5378 AS DOUBLE) AS bhck5378,
    CAST(bhck5379 AS DOUBLE) AS bhck5379,
    CAST(bhck5380 AS DOUBLE) AS bhck5380,
    CAST(bhck5381 AS DOUBLE) AS bhck5381,
    CAST(bhck5382 AS DOUBLE) AS bhck5382,
    CAST(bhck5383 AS BOOLEAN) AS bhck5383,
    CAST(bhck5384 AS DOUBLE) AS bhck5384,
    CAST(bhck5385 AS DOUBLE) AS bhck5385,
    CAST(bhck5386 AS BOOLEAN) AS bhck5386,
    CAST(bhck5387 AS DOUBLE) AS bhck5387,
    CAST(bhck5388 AS DOUBLE) AS bhck5388,
    CAST(bhck5389 AS DOUBLE) AS bhck5389,
    CAST(bhck5390 AS DOUBLE) AS bhck5390,
    CAST(bhck5391 AS DOUBLE) AS bhck5391,
    CAST(bhck5393 AS DOUBLE) AS bhck5393,
    CAST(bhck5397 AS DOUBLE) AS bhck5397,
    CAST(bhck5398 AS DOUBLE) AS bhck5398,
    CAST(bhck5399 AS DOUBLE) AS bhck5399,
    CAST(bhck5400 AS DOUBLE) AS bhck5400,
    CAST(bhck5401 AS DOUBLE) AS bhck5401,
    CAST(bhck5402 AS DOUBLE) AS bhck5402,
    CAST(bhck5403 AS DOUBLE) AS bhck5403,
    CAST(bhck5409 AS DOUBLE) AS bhck5409,
    CAST(bhck5411 AS DOUBLE) AS bhck5411,
    CAST(bhck5413 AS DOUBLE) AS bhck5413,
    CAST(bhck5459 AS DOUBLE) AS bhck5459,
    CAST(bhck5460 AS DOUBLE) AS bhck5460,
    CAST(bhck5461 AS DOUBLE) AS bhck5461,
    CAST(bhck5507 AS DOUBLE) AS bhck5507,
    CAST(bhck5610 AS DOUBLE) AS bhck5610,
    CAST(bhck5612 AS DOUBLE) AS bhck5612,
    CAST(bhck5613 AS DOUBLE) AS bhck5613,
    CAST(bhck5614 AS DOUBLE) AS bhck5614,
    CAST(bhck5615 AS DOUBLE) AS bhck5615,
    CAST(bhck5616 AS DOUBLE) AS bhck5616,
    CAST(bhck5617 AS DOUBLE) AS bhck5617,
    CAST(bhck6019 AS DOUBLE) AS bhck6019,
    CAST(bhck6373 AS DOUBLE) AS bhck6373,
    CAST(bhck6416 AS DOUBLE) AS bhck6416,
    CAST(bhck6438 AS DOUBLE) AS bhck6438,
    CAST(bhck6441 AS DOUBLE) AS bhck6441,
    CAST(bhck6442 AS DOUBLE) AS bhck6442,
    CAST(bhck6550 AS DOUBLE) AS bhck6550,
    CAST(bhck6555 AS DOUBLE) AS bhck6555,
    CAST(bhck6556 AS DOUBLE) AS bhck6556,
    CAST(bhck6557 AS DOUBLE) AS bhck6557,
    CAST(bhck6558 AS DOUBLE) AS bhck6558,
    CAST(bhck6559 AS DOUBLE) AS bhck6559,
    CAST(bhck6560 AS DOUBLE) AS bhck6560,
    CAST(bhck6561 AS DOUBLE) AS bhck6561,
    CAST(bhck6566 AS DOUBLE) AS bhck6566,
    CAST(bhck6572 AS DOUBLE) AS bhck6572,
    CAST(bhck6586 AS DOUBLE) AS bhck6586,
    CAST(bhck6599 AS DOUBLE) AS bhck6599,
    CAST(bhck6649 AS DOUBLE) AS bhck6649,
    CAST(bhck6669 AS BOOLEAN) AS bhck6669,
    CAST(bhck6688 AS DOUBLE) AS bhck6688,
    CAST(bhck6689 AS DOUBLE) AS bhck6689,
    CAST(bhck6761 AS DOUBLE) AS bhck6761,
    CAST(bhck6765 AS DOUBLE) AS bhck6765,
    CAST(bhck6927 AS BOOLEAN) AS bhck6927,
    CAST(bhck6928 AS BOOLEAN) AS bhck6928,
    CAST(bhck6995 AS BOOLEAN) AS bhck6995,
    CAST(bhck6998 AS BOOLEAN) AS bhck6998,
    CAST(bhck8403 AS DOUBLE) AS bhck8403,
    CAST(bhck8427 AS DOUBLE) AS bhck8427,
    CAST(bhck8428 AS DOUBLE) AS bhck8428,
    CAST(bhck8429 AS DOUBLE) AS bhck8429,
    CAST(bhck8430 AS DOUBLE) AS bhck8430,
    CAST(bhck8431 AS DOUBLE) AS bhck8431,
    CAST(bhck8433 AS DOUBLE) AS bhck8433,
    CAST(bhck8434 AS DOUBLE) AS bhck8434,
    CAST(bhck8492 AS DOUBLE) AS bhck8492,
    CAST(bhck8493 AS DOUBLE) AS bhck8493,
    CAST(bhck8494 AS DOUBLE) AS bhck8494,
    CAST(bhck8495 AS DOUBLE) AS bhck8495,
    CAST(bhck8496 AS DOUBLE) AS bhck8496,
    CAST(bhck8497 AS DOUBLE) AS bhck8497,
    CAST(bhck8498 AS DOUBLE) AS bhck8498,
    CAST(bhck8499 AS DOUBLE) AS bhck8499,
    CAST(bhck8531 AS DOUBLE) AS bhck8531,
    CAST(bhck8532 AS DOUBLE) AS bhck8532,
    CAST(bhck8533 AS DOUBLE) AS bhck8533,
    CAST(bhck8534 AS DOUBLE) AS bhck8534,
    CAST(bhck8535 AS DOUBLE) AS bhck8535,
    CAST(bhck8536 AS DOUBLE) AS bhck8536,
    CAST(bhck8537 AS DOUBLE) AS bhck8537,
    CAST(bhck8538 AS DOUBLE) AS bhck8538,
    CAST(bhck8539 AS DOUBLE) AS bhck8539,
    CAST(bhck8540 AS DOUBLE) AS bhck8540,
    CAST(bhck8541 AS DOUBLE) AS bhck8541,
    CAST(bhck8542 AS DOUBLE) AS bhck8542,
    CAST(bhck8543 AS DOUBLE) AS bhck8543,
    CAST(bhck8544 AS DOUBLE) AS bhck8544,
    CAST(bhck8545 AS DOUBLE) AS bhck8545,
    CAST(bhck8546 AS DOUBLE) AS bhck8546,
    CAST(bhck8547 AS DOUBLE) AS bhck8547,
    CAST(bhck8548 AS DOUBLE) AS bhck8548,
    CAST(bhck8549 AS DOUBLE) AS bhck8549,
    CAST(bhck8550 AS DOUBLE) AS bhck8550,
    CAST(bhck8557 AS DOUBLE) AS bhck8557,
    CAST(bhck8558 AS DOUBLE) AS bhck8558,
    CAST(bhck8559 AS DOUBLE) AS bhck8559,
    CAST(bhck8560 AS DOUBLE) AS bhck8560,
    CAST(bhck8561 AS DOUBLE) AS bhck8561,
    CAST(bhck8562 AS DOUBLE) AS bhck8562,
    CAST(bhck8563 AS DOUBLE) AS bhck8563,
    CAST(bhck8564 AS DOUBLE) AS bhck8564,
    CAST(bhck8565 AS DOUBLE) AS bhck8565,
    CAST(bhck8566 AS DOUBLE) AS bhck8566,
    CAST(bhck8567 AS DOUBLE) AS bhck8567,
    CAST(bhck8693 AS DOUBLE) AS bhck8693,
    CAST(bhck8694 AS DOUBLE) AS bhck8694,
    CAST(bhck8695 AS DOUBLE) AS bhck8695,
    CAST(bhck8696 AS DOUBLE) AS bhck8696,
    CAST(bhck8697 AS DOUBLE) AS bhck8697,
    CAST(bhck8698 AS DOUBLE) AS bhck8698,
    CAST(bhck8699 AS DOUBLE) AS bhck8699,
    CAST(bhck8700 AS DOUBLE) AS bhck8700,
    CAST(bhck8719 AS DOUBLE) AS bhck8719,
    CAST(bhck8720 AS DOUBLE) AS bhck8720,
    CAST(bhck8733 AS DOUBLE) AS bhck8733,
    CAST(bhck8734 AS DOUBLE) AS bhck8734,
    CAST(bhck8735 AS DOUBLE) AS bhck8735,
    CAST(bhck8736 AS DOUBLE) AS bhck8736,
    CAST(bhck8737 AS DOUBLE) AS bhck8737,
    CAST(bhck8738 AS DOUBLE) AS bhck8738,
    CAST(bhck8739 AS DOUBLE) AS bhck8739,
    CAST(bhck8740 AS DOUBLE) AS bhck8740,
    CAST(bhck8741 AS DOUBLE) AS bhck8741,
    CAST(bhck8742 AS DOUBLE) AS bhck8742,
    CAST(bhck8743 AS DOUBLE) AS bhck8743,
    CAST(bhck8744 AS DOUBLE) AS bhck8744,
    CAST(bhck8745 AS DOUBLE) AS bhck8745,
    CAST(bhck8746 AS DOUBLE) AS bhck8746,
    CAST(bhck8747 AS DOUBLE) AS bhck8747,
    CAST(bhck8748 AS DOUBLE) AS bhck8748,
    CAST(bhck8749 AS DOUBLE) AS bhck8749,
    CAST(bhck8750 AS DOUBLE) AS bhck8750,
    CAST(bhck8751 AS DOUBLE) AS bhck8751,
    CAST(bhck8752 AS DOUBLE) AS bhck8752,
    CAST(bhck8753 AS DOUBLE) AS bhck8753,
    CAST(bhck8754 AS DOUBLE) AS bhck8754,
    CAST(bhck8755 AS DOUBLE) AS bhck8755,
    CAST(bhck8756 AS DOUBLE) AS bhck8756,
    CAST(bhck8757 AS DOUBLE) AS bhck8757,
    CAST(bhck8758 AS DOUBLE) AS bhck8758,
    CAST(bhck8759 AS DOUBLE) AS bhck8759,
    CAST(bhck8760 AS DOUBLE) AS bhck8760,
    CAST(bhck8761 AS DOUBLE) AS bhck8761,
    CAST(bhck8762 AS DOUBLE) AS bhck8762,
    CAST(bhck8763 AS DOUBLE) AS bhck8763,
    CAST(bhck8764 AS DOUBLE) AS bhck8764,
    CAST(bhck8766 AS DOUBLE) AS bhck8766,
    CAST(bhck8767 AS DOUBLE) AS bhck8767,
    CAST(bhck8769 AS DOUBLE) AS bhck8769,
    CAST(bhck8770 AS DOUBLE) AS bhck8770,
    CAST(bhck8771 AS DOUBLE) AS bhck8771,
    CAST(bhck8772 AS DOUBLE) AS bhck8772,
    CAST(bhck8773 AS DOUBLE) AS bhck8773,
    CAST(bhck8774 AS DOUBLE) AS bhck8774,
    CAST(bhck8775 AS DOUBLE) AS bhck8775,
    CAST(bhck8776 AS DOUBLE) AS bhck8776,
    CAST(bhck8777 AS DOUBLE) AS bhck8777,
    CAST(bhck8778 AS DOUBLE) AS bhck8778,
    CAST(bhck8779 AS DOUBLE) AS bhck8779,
    CAST(bhck8782 AS DOUBLE) AS bhck8782,
    CAST(bhck8783 AS DOUBLE) AS bhck8783,
    CAST(bhck8843 AS DOUBLE) AS bhck8843,
    CAST(bhcka000 AS DOUBLE) AS bhcka000,
    CAST(bhcka001 AS DOUBLE) AS bhcka001,
    CAST(bhcka002 AS DOUBLE) AS bhcka002,
    CAST(bhcka130 AS DOUBLE) AS bhcka130,
    CAST(bhcka221 AS DOUBLE) AS bhcka221,
    CAST(bhcka222 AS DOUBLE) AS bhcka222,
    CAST(bhcka224 AS DOUBLE) AS bhcka224,
    CAST(bhcka250 AS DOUBLE) AS bhcka250,
    CAST(bhcka251 AS DOUBLE) AS bhcka251,
    CAST(bhcka506 AS DOUBLE) AS bhcka506,
    CAST(bhcka507 AS DOUBLE) AS bhcka507,
    CAST(bhcka510 AS DOUBLE) AS bhcka510,
    CAST(bhcka511 AS DOUBLE) AS bhcka511,
    CAST(bhcka512 AS DOUBLE) AS bhcka512,
    CAST(bhcka517 AS DOUBLE) AS bhcka517,
    CAST(bhcka518 AS DOUBLE) AS bhcka518,
    CAST(bhcka519 AS DOUBLE) AS bhcka519,
    CAST(bhcka520 AS DOUBLE) AS bhcka520,
    CAST(bhcka521 AS DOUBLE) AS bhcka521,
    CAST(bhcka522 AS DOUBLE) AS bhcka522,
    CAST(bhcka523 AS DOUBLE) AS bhcka523,
    CAST(bhcka524 AS DOUBLE) AS bhcka524,
    CAST(bhcka525 AS DOUBLE) AS bhcka525,
    CAST(bhcka530 AS DOUBLE) AS bhcka530,
    CAST(bhcka534 AS DOUBLE) AS bhcka534,
    CAST(bhcka535 AS DOUBLE) AS bhcka535,
    CAST(bhckb026 AS DOUBLE) AS bhckb026,
    CAST(bhckb029 AS DOUBLE) AS bhckb029,
    CAST(bhckb030 AS DOUBLE) AS bhckb030,
    CAST(bhckb032 AS DOUBLE) AS bhckb032,
    CAST(bhckb035 AS DOUBLE) AS bhckb035,
    CAST(bhckb036 AS DOUBLE) AS bhckb036,
    CAST(bhckb039 AS DOUBLE) AS bhckb039,
    CAST(bhckb040 AS DOUBLE) AS bhckb040,
    CAST(bhckb044 AS DOUBLE) AS bhckb044,
    CAST(bhckb045 AS DOUBLE) AS bhckb045,
    CAST(bhckb047 AS DOUBLE) AS bhckb047,
    CAST(bhckb050 AS DOUBLE) AS bhckb050,
    CAST(bhckb051 AS DOUBLE) AS bhckb051,
    CAST(bhckb054 AS DOUBLE) AS bhckb054,
    CAST(bhckb055 AS DOUBLE) AS bhckb055,
    CAST(bhckb077 AS DOUBLE) AS bhckb077,
    CAST(bhckb488 AS DOUBLE) AS bhckb488,
    CAST(bhckb489 AS DOUBLE) AS bhckb489,
    CAST(bhckb490 AS DOUBLE) AS bhckb490,
    CAST(bhckb492 AS DOUBLE) AS bhckb492,
    CAST(bhckb493 AS DOUBLE) AS bhckb493,
    CAST(bhckb494 AS DOUBLE) AS bhckb494,
    CAST(bhckb496 AS DOUBLE) AS bhckb496,
    CAST(bhckb497 AS DOUBLE) AS bhckb497,
    CAST(bhckb500 AS DOUBLE) AS bhckb500,
    CAST(bhckb501 AS DOUBLE) AS bhckb501,
    CAST(bhckb502 AS DOUBLE) AS bhckb502,
    CAST(bhckb508 AS DOUBLE) AS bhckb508,
    CAST(bhckb511 AS DOUBLE) AS bhckb511,
    CAST(bhckb512 AS DOUBLE) AS bhckb512,
    CAST(bhckb514 AS DOUBLE) AS bhckb514,
    CAST(bhckb516 AS DOUBLE) AS bhckb516,
    CAST(bhckb522 AS DOUBLE) AS bhckb522,
    CAST(bhckb528 AS DOUBLE) AS bhckb528,
    CAST(bhckb529 AS DOUBLE) AS bhckb529,
    CAST(bhckb530 AS DOUBLE) AS bhckb530,
    CAST(bhckb538 AS DOUBLE) AS bhckb538,
    CAST(bhckb539 AS DOUBLE) AS bhckb539,
    CAST(bhckb546 AS DOUBLE) AS bhckb546,
    CAST(bhckb556 AS DOUBLE) AS bhckb556,
    CAST(bhckb557 AS DOUBLE) AS bhckb557,
    CAST(bhckb559 AS DOUBLE) AS bhckb559,
    CAST(bhckb560 AS DOUBLE) AS bhckb560,
    CAST(bhckb569 AS DOUBLE) AS bhckb569,
    CAST(bhckb570 AS DOUBLE) AS bhckb570,
    CAST(bhckb572 AS DOUBLE) AS bhckb572,
    CAST(bhckb573 AS DOUBLE) AS bhckb573,
    CAST(bhckb574 AS DOUBLE) AS bhckb574,
    CAST(bhckb575 AS DOUBLE) AS bhckb575,
    CAST(bhckb576 AS DOUBLE) AS bhckb576,
    CAST(bhckb577 AS DOUBLE) AS bhckb577,
    CAST(bhckb578 AS DOUBLE) AS bhckb578,
    CAST(bhckb579 AS DOUBLE) AS bhckb579,
    CAST(bhckb580 AS DOUBLE) AS bhckb580,
    CAST(bhckb588 AS DOUBLE) AS bhckb588,
    CAST(bhckb590 AS DOUBLE) AS bhckb590,
    CAST(bhckb591 AS DOUBLE) AS bhckb591,
    CAST(bhckb592 AS DOUBLE) AS bhckb592,
    CAST(bhckb593 AS DOUBLE) AS bhckb593,
    CAST(bhckb594 AS DOUBLE) AS bhckb594,
    CAST(bhckb595 AS DOUBLE) AS bhckb595,
    CAST(bhckb596 AS DOUBLE) AS bhckb596,
    CAST(bhckb639 AS DOUBLE) AS bhckb639,
    CAST(bhckb675 AS DOUBLE) AS bhckb675,
    CAST(bhckb681 AS DOUBLE) AS bhckb681,
    CAST(bhckb747 AS DOUBLE) AS bhckb747,
    CAST(bhckb748 AS DOUBLE) AS bhckb748,
    CAST(bhckb749 AS DOUBLE) AS bhckb749,
    CAST(bhckb750 AS DOUBLE) AS bhckb750,
    CAST(bhckb751 AS DOUBLE) AS bhckb751,
    CAST(bhckb752 AS DOUBLE) AS bhckb752,
    CAST(bhckb753 AS DOUBLE) AS bhckb753,
    CAST(bhckb761 AS DOUBLE) AS bhckb761,
    CAST(bhckb762 AS DOUBLE) AS bhckb762,
    CAST(bhckb763 AS DOUBLE) AS bhckb763,
    CAST(bhckb770 AS DOUBLE) AS bhckb770,
    CAST(bhckb771 AS DOUBLE) AS bhckb771,
    CAST(bhckb772 AS DOUBLE) AS bhckb772,
    CAST(bhckb776 AS DOUBLE) AS bhckb776,
    CAST(bhckb777 AS DOUBLE) AS bhckb777,
    CAST(bhckb778 AS DOUBLE) AS bhckb778,
    CAST(bhckb779 AS DOUBLE) AS bhckb779,
    CAST(bhckb780 AS DOUBLE) AS bhckb780,
    CAST(bhckb781 AS DOUBLE) AS bhckb781,
    CAST(bhckb782 AS DOUBLE) AS bhckb782,
    CAST(bhckb790 AS DOUBLE) AS bhckb790,
    CAST(bhckb791 AS DOUBLE) AS bhckb791,
    CAST(bhckb792 AS DOUBLE) AS bhckb792,
    CAST(bhckb793 AS DOUBLE) AS bhckb793,
    CAST(bhckb794 AS DOUBLE) AS bhckb794,
    CAST(bhckb795 AS DOUBLE) AS bhckb795,
    CAST(bhckb796 AS DOUBLE) AS bhckb796,
    CAST(bhckb797 AS DOUBLE) AS bhckb797,
    CAST(bhckb798 AS DOUBLE) AS bhckb798,
    CAST(bhckb799 AS DOUBLE) AS bhckb799,
    CAST(bhckb800 AS DOUBLE) AS bhckb800,
    CAST(bhckb801 AS DOUBLE) AS bhckb801,
    CAST(bhckb802 AS DOUBLE) AS bhckb802,
    CAST(bhckb803 AS DOUBLE) AS bhckb803,
    CAST(bhckb806 AS DOUBLE) AS bhckb806,
    CAST(bhckb807 AS DOUBLE) AS bhckb807,
    CAST(bhckb837 AS DOUBLE) AS bhckb837,
    CAST(bhckb838 AS DOUBLE) AS bhckb838,
    CAST(bhckb839 AS DOUBLE) AS bhckb839,
    CAST(bhckb840 AS DOUBLE) AS bhckb840,
    CAST(bhckb841 AS DOUBLE) AS bhckb841,
    CAST(bhckb842 AS DOUBLE) AS bhckb842,
    CAST(bhckb843 AS DOUBLE) AS bhckb843,
    CAST(bhckb844 AS DOUBLE) AS bhckb844,
    CAST(bhckb845 AS DOUBLE) AS bhckb845,
    CAST(bhckb846 AS DOUBLE) AS bhckb846,
    CAST(bhckb847 AS DOUBLE) AS bhckb847,
    CAST(bhckb848 AS DOUBLE) AS bhckb848,
    CAST(bhckb849 AS DOUBLE) AS bhckb849,
    CAST(bhckb850 AS DOUBLE) AS bhckb850,
    CAST(bhckb851 AS DOUBLE) AS bhckb851,
    CAST(bhckb852 AS DOUBLE) AS bhckb852,
    CAST(bhckb853 AS DOUBLE) AS bhckb853,
    CAST(bhckb854 AS DOUBLE) AS bhckb854,
    CAST(bhckb855 AS DOUBLE) AS bhckb855,
    CAST(bhckb856 AS DOUBLE) AS bhckb856,
    CAST(bhckb857 AS DOUBLE) AS bhckb857,
    CAST(bhckb858 AS DOUBLE) AS bhckb858,
    CAST(bhckb859 AS DOUBLE) AS bhckb859,
    CAST(bhckb860 AS DOUBLE) AS bhckb860,
    CAST(bhckb861 AS DOUBLE) AS bhckb861,
    CAST(bhckb983 AS DOUBLE) AS bhckb983,
    CAST(bhckb984 AS DOUBLE) AS bhckb984,
    CAST(bhckb985 AS DOUBLE) AS bhckb985,
    CAST(bhckb986 AS BOOLEAN) AS bhckb986,
    CAST(bhckb988 AS DOUBLE) AS bhckb988,
    CAST(bhckb990 AS DOUBLE) AS bhckb990,
    CAST(bhckb991 AS DOUBLE) AS bhckb991,
    CAST(bhckb992 AS DOUBLE) AS bhckb992,
    CAST(bhckb994 AS DOUBLE) AS bhckb994,
    CAST(bhckb996 AS DOUBLE) AS bhckb996,
    CAST(bhckb998 AS DOUBLE) AS bhckb998,
    CAST(bhckc009 AS DOUBLE) AS bhckc009,
    CAST(bhckc013 AS DOUBLE) AS bhckc013,
    CAST(bhckc014 AS DOUBLE) AS bhckc014,
    CAST(bhckc016 AS DOUBLE) AS bhckc016,
    CAST(bhckc017 AS DOUBLE) AS bhckc017,
    CAST(bhckc050 AS BOOLEAN) AS bhckc050,
    CAST(bhckc079 AS DOUBLE) AS bhckc079,
    CAST(bhckc159 AS DOUBLE) AS bhckc159,
    CAST(bhckc160 AS DOUBLE) AS bhckc160,
    CAST(bhckc161 AS DOUBLE) AS bhckc161,
    CAST(bhckc216 AS DOUBLE) AS bhckc216,
    CAST(bhckc219 AS DOUBLE) AS bhckc219,
    CAST(bhckc220 AS DOUBLE) AS bhckc220,
    CAST(bhckc221 AS DOUBLE) AS bhckc221,
    CAST(bhckc222 AS DOUBLE) AS bhckc222,
    CAST(bhckc225 AS DOUBLE) AS bhckc225,
    CAST(bhckc226 AS DOUBLE) AS bhckc226,
    CAST(bhckc229 AS DOUBLE) AS bhckc229,
    CAST(bhckc230 AS DOUBLE) AS bhckc230,
    CAST(bhckc231 AS DOUBLE) AS bhckc231,
    CAST(bhckc232 AS DOUBLE) AS bhckc232,
    CAST(bhckc233 AS DOUBLE) AS bhckc233,
    CAST(bhckc234 AS DOUBLE) AS bhckc234,
    CAST(bhckc235 AS DOUBLE) AS bhckc235,
    CAST(bhckc236 AS DOUBLE) AS bhckc236,
    CAST(bhckc237 AS DOUBLE) AS bhckc237,
    CAST(bhckc238 AS DOUBLE) AS bhckc238,
    CAST(bhckc239 AS DOUBLE) AS bhckc239,
    CAST(bhckc240 AS DOUBLE) AS bhckc240,
    CAST(bhckc241 AS DOUBLE) AS bhckc241,
    CAST(bhckc243 AS DOUBLE) AS bhckc243,
    CAST(bhckc246 AS DOUBLE) AS bhckc246,
    CAST(bhckc250 AS DOUBLE) AS bhckc250,
    CAST(bhckc251 AS DOUBLE) AS bhckc251,
    CAST(bhckc252 AS DOUBLE) AS bhckc252,
    CAST(bhckc253 AS DOUBLE) AS bhckc253,
    CAST(bhckc386 AS DOUBLE) AS bhckc386,
    CAST(bhckc387 AS DOUBLE) AS bhckc387,
    CAST(bhckc390 AS DOUBLE) AS bhckc390,
    CAST(bhckc410 AS DOUBLE) AS bhckc410,
    CAST(bhckc411 AS DOUBLE) AS bhckc411,
    CAST(bhckc435 AS DOUBLE) AS bhckc435,
    CAST(bhckc447 AS DOUBLE) AS bhckc447,
    CAST(bhckc498 AS DOUBLE) AS bhckc498,
    CAST(bhckc700 AS DOUBLE) AS bhckc700,
    CAST(bhckc701 AS DOUBLE) AS bhckc701,
    CAST(bhckc781 AS DOUBLE) AS bhckc781,
    CAST(bhckc880 AS DOUBLE) AS bhckc880,
    CAST(bhckc884 AS DOUBLE) AS bhckc884,
    CAST(bhckc886 AS DOUBLE) AS bhckc886,
    CAST(bhckc887 AS DOUBLE) AS bhckc887,
    CAST(bhckc888 AS DOUBLE) AS bhckc888,
    CAST(bhckc889 AS DOUBLE) AS bhckc889,
    CAST(bhckc890 AS DOUBLE) AS bhckc890,
    CAST(bhckc891 AS DOUBLE) AS bhckc891,
    CAST(bhckc892 AS DOUBLE) AS bhckc892,
    CAST(bhckc893 AS DOUBLE) AS bhckc893,
    CAST(bhckc894 AS DOUBLE) AS bhckc894,
    CAST(bhckc895 AS DOUBLE) AS bhckc895,
    CAST(bhckc896 AS DOUBLE) AS bhckc896,
    CAST(bhckc897 AS DOUBLE) AS bhckc897,
    CAST(bhckc898 AS DOUBLE) AS bhckc898,
    CAST(bhckc968 AS DOUBLE) AS bhckc968,
    CAST(bhckc969 AS DOUBLE) AS bhckc969,
    CAST(bhckc970 AS DOUBLE) AS bhckc970,
    CAST(bhckc971 AS DOUBLE) AS bhckc971,
    CAST(bhckc972 AS DOUBLE) AS bhckc972,
    CAST(bhckc973 AS DOUBLE) AS bhckc973,
    CAST(bhckc974 AS DOUBLE) AS bhckc974,
    CAST(bhckc975 AS DOUBLE) AS bhckc975,
    CAST(bhckc980 AS DOUBLE) AS bhckc980,
    CAST(bhckc981 AS DOUBLE) AS bhckc981,
    CAST(bhckc982 AS DOUBLE) AS bhckc982,
    CAST(bhckc983 AS DOUBLE) AS bhckc983,
    CAST(bhckc984 AS DOUBLE) AS bhckc984,
    CAST(bhckc985 AS DOUBLE) AS bhckc985,
    CAST(bhckc988 AS DOUBLE) AS bhckc988,
    CAST(bhckc989 AS DOUBLE) AS bhckc989,
    CAST(bhckd958 AS DOUBLE) AS bhckd958,
    CAST(bhckd959 AS DOUBLE) AS bhckd959,
    CAST(bhckd960 AS DOUBLE) AS bhckd960,
    CAST(bhckd962 AS DOUBLE) AS bhckd962,
    CAST(bhckd963 AS DOUBLE) AS bhckd963,
    CAST(bhckd964 AS DOUBLE) AS bhckd964,
    CAST(bhckd965 AS DOUBLE) AS bhckd965,
    CAST(bhckd967 AS DOUBLE) AS bhckd967,
    CAST(bhckd968 AS DOUBLE) AS bhckd968,
    CAST(bhckd969 AS DOUBLE) AS bhckd969,
    CAST(bhckd970 AS DOUBLE) AS bhckd970,
    CAST(bhckd971 AS DOUBLE) AS bhckd971,
    CAST(bhckd972 AS DOUBLE) AS bhckd972,
    CAST(bhckd973 AS DOUBLE) AS bhckd973,
    CAST(bhckd974 AS DOUBLE) AS bhckd974,
    CAST(bhckd982 AS DOUBLE) AS bhckd982,
    CAST(bhckd983 AS DOUBLE) AS bhckd983,
    CAST(bhckd984 AS DOUBLE) AS bhckd984,
    CAST(bhckd985 AS DOUBLE) AS bhckd985,
    CAST(bhckd991 AS DOUBLE) AS bhckd991,
    CAST(bhckd992 AS DOUBLE) AS bhckd992,
    CAST(bhckd993 AS DOUBLE) AS bhckd993,
    CAST(bhckd994 AS DOUBLE) AS bhckd994,
    CAST(bhckd995 AS DOUBLE) AS bhckd995,
    CAST(bhckd996 AS DOUBLE) AS bhckd996,
    CAST(bhckf031 AS DOUBLE) AS bhckf031,
    CAST(bhckf070 AS DOUBLE) AS bhckf070,
    CAST(bhckf071 AS DOUBLE) AS bhckf071,
    CAST(bhckf072 AS DOUBLE) AS bhckf072,
    CAST(bhckf073 AS DOUBLE) AS bhckf073,
    CAST(bhckf158 AS DOUBLE) AS bhckf158,
    CAST(bhckf159 AS DOUBLE) AS bhckf159,
    CAST(bhckf160 AS DOUBLE) AS bhckf160,
    CAST(bhckf161 AS DOUBLE) AS bhckf161,
    CAST(bhckf162 AS DOUBLE) AS bhckf162,
    CAST(bhckf163 AS DOUBLE) AS bhckf163,
    CAST(bhckf164 AS DOUBLE) AS bhckf164,
    CAST(bhckf165 AS DOUBLE) AS bhckf165,
    CAST(bhckf166 AS DOUBLE) AS bhckf166,
    CAST(bhckf167 AS DOUBLE) AS bhckf167,
    CAST(bhckf168 AS DOUBLE) AS bhckf168,
    CAST(bhckf169 AS DOUBLE) AS bhckf169,
    CAST(bhckf170 AS DOUBLE) AS bhckf170,
    CAST(bhckf171 AS DOUBLE) AS bhckf171,
    CAST(bhckf172 AS DOUBLE) AS bhckf172,
    CAST(bhckf173 AS DOUBLE) AS bhckf173,
    CAST(bhckf174 AS DOUBLE) AS bhckf174,
    CAST(bhckf175 AS DOUBLE) AS bhckf175,
    CAST(bhckf176 AS DOUBLE) AS bhckf176,
    CAST(bhckf177 AS DOUBLE) AS bhckf177,
    CAST(bhckf178 AS DOUBLE) AS bhckf178,
    CAST(bhckf179 AS DOUBLE) AS bhckf179,
    CAST(bhckf180 AS DOUBLE) AS bhckf180,
    CAST(bhckf181 AS DOUBLE) AS bhckf181,
    CAST(bhckf182 AS DOUBLE) AS bhckf182,
    CAST(bhckf183 AS DOUBLE) AS bhckf183,
    CAST(bhckf184 AS DOUBLE) AS bhckf184,
    CAST(bhckf185 AS DOUBLE) AS bhckf185,
    CAST(bhckf228 AS DOUBLE) AS bhckf228,
    CAST(bhckf229 AS DOUBLE) AS bhckf229,
    CAST(bhckf241 AS DOUBLE) AS bhckf241,
    CAST(bhckf242 AS DOUBLE) AS bhckf242,
    CAST(bhckf244 AS DOUBLE) AS bhckf244,
    CAST(bhckf245 AS DOUBLE) AS bhckf245,
    CAST(bhckf247 AS DOUBLE) AS bhckf247,
    CAST(bhckf248 AS DOUBLE) AS bhckf248,
    CAST(bhckf250 AS DOUBLE) AS bhckf250,
    CAST(bhckf251 AS DOUBLE) AS bhckf251,
    CAST(bhckf253 AS DOUBLE) AS bhckf253,
    CAST(bhckf254 AS DOUBLE) AS bhckf254,
    CAST(bhckf256 AS DOUBLE) AS bhckf256,
    CAST(bhckf257 AS DOUBLE) AS bhckf257,
    CAST(bhckf259 AS DOUBLE) AS bhckf259,
    CAST(bhckf260 AS DOUBLE) AS bhckf260,
    CAST(bhckf262 AS DOUBLE) AS bhckf262,
    CAST(bhckf263 AS DOUBLE) AS bhckf263,
    CAST(bhckf264 AS DOUBLE) AS bhckf264,
    CAST(bhckf465 AS DOUBLE) AS bhckf465,
    CAST(bhckf551 AS DOUBLE) AS bhckf551,
    CAST(bhckf552 AS DOUBLE) AS bhckf552,
    CAST(bhckf553 AS DOUBLE) AS bhckf553,
    CAST(bhckf554 AS DOUBLE) AS bhckf554,
    CAST(bhckf555 AS DOUBLE) AS bhckf555,
    CAST(bhckf556 AS DOUBLE) AS bhckf556,
    CAST(bhckf557 AS DOUBLE) AS bhckf557,
    CAST(bhckf558 AS DOUBLE) AS bhckf558,
    CAST(bhckf585 AS DOUBLE) AS bhckf585,
    CAST(bhckf586 AS DOUBLE) AS bhckf586,
    CAST(bhckf587 AS DOUBLE) AS bhckf587,
    CAST(bhckf588 AS DOUBLE) AS bhckf588,
    CAST(bhckf589 AS DOUBLE) AS bhckf589,
    CAST(bhckf608 AS DOUBLE) AS bhckf608,
    CAST(bhckf639 AS DOUBLE) AS bhckf639,
    CAST(bhckf640 AS DOUBLE) AS bhckf640,
    CAST(bhckf655 AS DOUBLE) AS bhckf655,
    CAST(bhckf658 AS DOUBLE) AS bhckf658,
    CAST(bhckf661 AS DOUBLE) AS bhckf661,
    CAST(bhckf662 AS DOUBLE) AS bhckf662,
    CAST(bhckf663 AS DOUBLE) AS bhckf663,
    CAST(bhckf664 AS DOUBLE) AS bhckf664,
    CAST(bhckf665 AS DOUBLE) AS bhckf665,
    CAST(bhckf666 AS DOUBLE) AS bhckf666,
    CAST(bhckf682 AS DOUBLE) AS bhckf682,
    CAST(bhckf683 AS DOUBLE) AS bhckf683,
    CAST(bhckf684 AS DOUBLE) AS bhckf684,
    CAST(bhckf685 AS DOUBLE) AS bhckf685,
    CAST(bhckf686 AS DOUBLE) AS bhckf686,
    CAST(bhckf687 AS DOUBLE) AS bhckf687,
    CAST(bhckf688 AS DOUBLE) AS bhckf688,
    CAST(bhckf689 AS DOUBLE) AS bhckf689,
    CAST(bhckf690 AS DOUBLE) AS bhckf690,
    CAST(bhckf691 AS DOUBLE) AS bhckf691,
    CAST(bhckf692 AS DOUBLE) AS bhckf692,
    CAST(bhckf693 AS DOUBLE) AS bhckf693,
    CAST(bhckf694 AS DOUBLE) AS bhckf694,
    CAST(bhckf695 AS DOUBLE) AS bhckf695,
    CAST(bhckf696 AS DOUBLE) AS bhckf696,
    CAST(bhckf697 AS DOUBLE) AS bhckf697,
    CAST(bhckf821 AS DOUBLE) AS bhckf821,
    CAST(bhckf841 AS BOOLEAN) AS bhckf841,
    CAST(bhckft28 AS DOUBLE) AS bhckft28,
    CAST(bhckft29 AS DOUBLE) AS bhckft29,
    CAST(bhckft30 AS DOUBLE) AS bhckft30,
    CAST(bhckft31 AS DOUBLE) AS bhckft31,
    CAST(bhckft32 AS DOUBLE) AS bhckft32,
    CAST(bhckft41 AS DOUBLE) AS bhckft41,
    CAST(bhckft42 AS BOOLEAN) AS bhckft42,
    CAST(bhckft43 AS BOOLEAN) AS bhckft43,
    CAST(bhckft44 AS BOOLEAN) AS bhckft44,
    CAST(bhckg091 AS DOUBLE) AS bhckg091,
    CAST(bhckg092 AS DOUBLE) AS bhckg092,
    CAST(bhckg093 AS DOUBLE) AS bhckg093,
    CAST(bhckg094 AS DOUBLE) AS bhckg094,
    CAST(bhckg095 AS DOUBLE) AS bhckg095,
    CAST(bhckg096 AS DOUBLE) AS bhckg096,
    CAST(bhckg097 AS DOUBLE) AS bhckg097,
    CAST(bhckg098 AS DOUBLE) AS bhckg098,
    CAST(bhckg099 AS DOUBLE) AS bhckg099,
    CAST(bhckg100 AS DOUBLE) AS bhckg100,
    CAST(bhckg101 AS DOUBLE) AS bhckg101,
    CAST(bhckg102 AS DOUBLE) AS bhckg102,
    CAST(bhckg103 AS DOUBLE) AS bhckg103,
    CAST(bhckg104 AS DOUBLE) AS bhckg104,
    CAST(bhckg209 AS DOUBLE) AS bhckg209,
    CAST(bhckg210 AS DOUBLE) AS bhckg210,
    CAST(bhckg211 AS DOUBLE) AS bhckg211,
    CAST(bhckg212 AS DOUBLE) AS bhckg212,
    CAST(bhckg213 AS DOUBLE) AS bhckg213,
    CAST(bhckg218 AS DOUBLE) AS bhckg218,
    CAST(bhckg221 AS DOUBLE) AS bhckg221,
    CAST(bhckg234 AS DOUBLE) AS bhckg234,
    CAST(bhckg235 AS DOUBLE) AS bhckg235,
    CAST(bhckg300 AS DOUBLE) AS bhckg300,
    CAST(bhckg301 AS DOUBLE) AS bhckg301,
    CAST(bhckg302 AS DOUBLE) AS bhckg302,
    CAST(bhckg303 AS DOUBLE) AS bhckg303,
    CAST(bhckg304 AS DOUBLE) AS bhckg304,
    CAST(bhckg305 AS DOUBLE) AS bhckg305,
    CAST(bhckg306 AS DOUBLE) AS bhckg306,
    CAST(bhckg307 AS DOUBLE) AS bhckg307,
    CAST(bhckg308 AS DOUBLE) AS bhckg308,
    CAST(bhckg309 AS DOUBLE) AS bhckg309,
    CAST(bhckg310 AS DOUBLE) AS bhckg310,
    CAST(bhckg311 AS DOUBLE) AS bhckg311,
    CAST(bhckg312 AS DOUBLE) AS bhckg312,
    CAST(bhckg313 AS DOUBLE) AS bhckg313,
    CAST(bhckg314 AS DOUBLE) AS bhckg314,
    CAST(bhckg315 AS DOUBLE) AS bhckg315,
    CAST(bhckg316 AS DOUBLE) AS bhckg316,
    CAST(bhckg317 AS DOUBLE) AS bhckg317,
    CAST(bhckg318 AS DOUBLE) AS bhckg318,
    CAST(bhckg319 AS DOUBLE) AS bhckg319,
    CAST(bhckg320 AS DOUBLE) AS bhckg320,
    CAST(bhckg321 AS DOUBLE) AS bhckg321,
    CAST(bhckg322 AS DOUBLE) AS bhckg322,
    CAST(bhckg323 AS DOUBLE) AS bhckg323,
    CAST(bhckg324 AS DOUBLE) AS bhckg324,
    CAST(bhckg325 AS DOUBLE) AS bhckg325,
    CAST(bhckg326 AS DOUBLE) AS bhckg326,
    CAST(bhckg327 AS DOUBLE) AS bhckg327,
    CAST(bhckg328 AS DOUBLE) AS bhckg328,
    CAST(bhckg329 AS DOUBLE) AS bhckg329,
    CAST(bhckg330 AS DOUBLE) AS bhckg330,
    CAST(bhckg331 AS DOUBLE) AS bhckg331,
    CAST(bhckg336 AS DOUBLE) AS bhckg336,
    CAST(bhckg337 AS DOUBLE) AS bhckg337,
    CAST(bhckg338 AS DOUBLE) AS bhckg338,
    CAST(bhckg339 AS DOUBLE) AS bhckg339,
    CAST(bhckg340 AS DOUBLE) AS bhckg340,
    CAST(bhckg341 AS DOUBLE) AS bhckg341,
    CAST(bhckg342 AS DOUBLE) AS bhckg342,
    CAST(bhckg343 AS DOUBLE) AS bhckg343,
    CAST(bhckg344 AS DOUBLE) AS bhckg344,
    CAST(bhckg345 AS DOUBLE) AS bhckg345,
    CAST(bhckg346 AS DOUBLE) AS bhckg346,
    CAST(bhckg347 AS DOUBLE) AS bhckg347,
    CAST(bhckg391 AS DOUBLE) AS bhckg391,
    CAST(bhckg392 AS DOUBLE) AS bhckg392,
    CAST(bhckg395 AS DOUBLE) AS bhckg395,
    CAST(bhckg396 AS DOUBLE) AS bhckg396,
    CAST(bhckg401 AS DOUBLE) AS bhckg401,
    CAST(bhckg402 AS DOUBLE) AS bhckg402,
    CAST(bhckg403 AS DOUBLE) AS bhckg403,
    CAST(bhckg404 AS DOUBLE) AS bhckg404,
    CAST(bhckg405 AS DOUBLE) AS bhckg405,
    CAST(bhckg406 AS DOUBLE) AS bhckg406,
    CAST(bhckg407 AS DOUBLE) AS bhckg407,
    CAST(bhckg408 AS DOUBLE) AS bhckg408,
    CAST(bhckg409 AS DOUBLE) AS bhckg409,
    CAST(bhckg410 AS DOUBLE) AS bhckg410,
    CAST(bhckg411 AS DOUBLE) AS bhckg411,
    CAST(bhckg412 AS DOUBLE) AS bhckg412,
    CAST(bhckg413 AS DOUBLE) AS bhckg413,
    CAST(bhckg414 AS DOUBLE) AS bhckg414,
    CAST(bhckg415 AS DOUBLE) AS bhckg415,
    CAST(bhckg416 AS DOUBLE) AS bhckg416,
    CAST(bhckg417 AS DOUBLE) AS bhckg417,
    CAST(bhckg474 AS DOUBLE) AS bhckg474,
    CAST(bhckg475 AS DOUBLE) AS bhckg475,
    CAST(bhckg476 AS DOUBLE) AS bhckg476,
    CAST(bhckg477 AS DOUBLE) AS bhckg477,
    CAST(bhckg478 AS DOUBLE) AS bhckg478,
    CAST(bhckg479 AS DOUBLE) AS bhckg479,
    CAST(bhckg480 AS DOUBLE) AS bhckg480,
    CAST(bhckg481 AS DOUBLE) AS bhckg481,
    CAST(bhckg482 AS DOUBLE) AS bhckg482,
    CAST(bhckg483 AS DOUBLE) AS bhckg483,
    CAST(bhckg484 AS DOUBLE) AS bhckg484,
    CAST(bhckg485 AS DOUBLE) AS bhckg485,
    CAST(bhckg486 AS DOUBLE) AS bhckg486,
    CAST(bhckg487 AS DOUBLE) AS bhckg487,
    CAST(bhckg488 AS DOUBLE) AS bhckg488,
    CAST(bhckg489 AS DOUBLE) AS bhckg489,
    CAST(bhckg490 AS DOUBLE) AS bhckg490,
    CAST(bhckg491 AS DOUBLE) AS bhckg491,
    CAST(bhckg492 AS DOUBLE) AS bhckg492,
    CAST(bhckg507 AS DOUBLE) AS bhckg507,
    CAST(bhckg508 AS DOUBLE) AS bhckg508,
    CAST(bhckg509 AS DOUBLE) AS bhckg509,
    CAST(bhckg510 AS DOUBLE) AS bhckg510,
    CAST(bhckg511 AS DOUBLE) AS bhckg511,
    CAST(bhckg521 AS DOUBLE) AS bhckg521,
    CAST(bhckg522 AS DOUBLE) AS bhckg522,
    CAST(bhckg523 AS DOUBLE) AS bhckg523,
    CAST(bhckg524 AS DOUBLE) AS bhckg524,
    CAST(bhckg525 AS DOUBLE) AS bhckg525,
    CAST(bhckg536 AS DOUBLE) AS bhckg536,
    CAST(bhckg537 AS DOUBLE) AS bhckg537,
    CAST(bhckg538 AS DOUBLE) AS bhckg538,
    CAST(bhckg539 AS DOUBLE) AS bhckg539,
    CAST(bhckg540 AS DOUBLE) AS bhckg540,
    CAST(bhckg541 AS DOUBLE) AS bhckg541,
    CAST(bhckg542 AS DOUBLE) AS bhckg542,
    CAST(bhckg543 AS DOUBLE) AS bhckg543,
    CAST(bhckg544 AS DOUBLE) AS bhckg544,
    CAST(bhckg545 AS DOUBLE) AS bhckg545,
    CAST(bhckg546 AS DOUBLE) AS bhckg546,
    CAST(bhckg547 AS DOUBLE) AS bhckg547,
    CAST(bhckg548 AS DOUBLE) AS bhckg548,
    CAST(bhckg549 AS DOUBLE) AS bhckg549,
    CAST(bhckg550 AS DOUBLE) AS bhckg550,
    CAST(bhckg561 AS DOUBLE) AS bhckg561,
    CAST(bhckg562 AS DOUBLE) AS bhckg562,
    CAST(bhckg563 AS DOUBLE) AS bhckg563,
    CAST(bhckg564 AS DOUBLE) AS bhckg564,
    CAST(bhckg565 AS DOUBLE) AS bhckg565,
    CAST(bhckg566 AS DOUBLE) AS bhckg566,
    CAST(bhckg567 AS DOUBLE) AS bhckg567,
    CAST(bhckg568 AS DOUBLE) AS bhckg568,
    CAST(bhckg569 AS DOUBLE) AS bhckg569,
    CAST(bhckg570 AS DOUBLE) AS bhckg570,
    CAST(bhckg571 AS DOUBLE) AS bhckg571,
    CAST(bhckg572 AS DOUBLE) AS bhckg572,
    CAST(bhckg573 AS DOUBLE) AS bhckg573,
    CAST(bhckg574 AS DOUBLE) AS bhckg574,
    CAST(bhckg575 AS DOUBLE) AS bhckg575,
    CAST(bhckg586 AS DOUBLE) AS bhckg586,
    CAST(bhckg587 AS DOUBLE) AS bhckg587,
    CAST(bhckg588 AS DOUBLE) AS bhckg588,
    CAST(bhckg589 AS DOUBLE) AS bhckg589,
    CAST(bhckg590 AS DOUBLE) AS bhckg590,
    CAST(bhckg597 AS DOUBLE) AS bhckg597,
    CAST(bhckg598 AS DOUBLE) AS bhckg598,
    CAST(bhckg599 AS DOUBLE) AS bhckg599,
    CAST(bhckg600 AS DOUBLE) AS bhckg600,
    CAST(bhckg601 AS DOUBLE) AS bhckg601,
    CAST(bhckg602 AS DOUBLE) AS bhckg602,
    CAST(bhckg606 AS DOUBLE) AS bhckg606,
    CAST(bhckg607 AS DOUBLE) AS bhckg607,
    CAST(bhckg608 AS DOUBLE) AS bhckg608,
    CAST(bhckg609 AS DOUBLE) AS bhckg609,
    CAST(bhckg610 AS DOUBLE) AS bhckg610,
    CAST(bhckg611 AS DOUBLE) AS bhckg611,
    CAST(bhckg618 AS DOUBLE) AS bhckg618,
    CAST(bhckg619 AS DOUBLE) AS bhckg619,
    CAST(bhckg620 AS DOUBLE) AS bhckg620,
    CAST(bhckg621 AS DOUBLE) AS bhckg621,
    CAST(bhckg622 AS DOUBLE) AS bhckg622,
    CAST(bhckg623 AS DOUBLE) AS bhckg623,
    CAST(bhckg642 AS DOUBLE) AS bhckg642,
    CAST(bhckg804 AS DOUBLE) AS bhckg804,
    CAST(bhckg805 AS DOUBLE) AS bhckg805,
    CAST(bhckg806 AS DOUBLE) AS bhckg806,
    CAST(bhckg807 AS DOUBLE) AS bhckg807,
    CAST(bhckg808 AS DOUBLE) AS bhckg808,
    CAST(bhckg809 AS DOUBLE) AS bhckg809,
    CAST(bhckg894 AS DOUBLE) AS bhckg894,
    CAST(bhckg914 AS DOUBLE) AS bhckg914,
    CAST(bhckh172 AS DOUBLE) AS bhckh172,
    CAST(bhckh173 AS DOUBLE) AS bhckh173,
    CAST(bhckh174 AS DOUBLE) AS bhckh174,
    CAST(bhckh175 AS DOUBLE) AS bhckh175,
    CAST(bhckh176 AS DOUBLE) AS bhckh176,
    CAST(bhckh177 AS DOUBLE) AS bhckh177,
    CAST(bhckh178 AS DOUBLE) AS bhckh178,
    CAST(bhckh179 AS DOUBLE) AS bhckh179,
    CAST(bhckh180 AS DOUBLE) AS bhckh180,
    CAST(bhckh181 AS DOUBLE) AS bhckh181,
    CAST(bhckh182 AS DOUBLE) AS bhckh182,
    CAST(bhckh185 AS DOUBLE) AS bhckh185,
    CAST(bhckh186 AS DOUBLE) AS bhckh186,
    CAST(bhckh187 AS DOUBLE) AS bhckh187,
    CAST(bhckh188 AS DOUBLE) AS bhckh188,
    CAST(bhckh193 AS DOUBLE) AS bhckh193,
    CAST(bhckh194 AS DOUBLE) AS bhckh194,
    CAST(bhckh195 AS DOUBLE) AS bhckh195,
    CAST(bhckh196 AS DOUBLE) AS bhckh196,
    CAST(bhckh197 AS DOUBLE) AS bhckh197,
    CAST(bhckh198 AS DOUBLE) AS bhckh198,
    CAST(bhckh199 AS DOUBLE) AS bhckh199,
    CAST(bhckh200 AS DOUBLE) AS bhckh200,
    CAST(bhckh270 AS DOUBLE) AS bhckh270,
    CAST(bhckh271 AS DOUBLE) AS bhckh271,
    CAST(bhckh272 AS DOUBLE) AS bhckh272,
    CAST(bhckh273 AS DOUBLE) AS bhckh273,
    CAST(bhckh274 AS DOUBLE) AS bhckh274,
    CAST(bhckh275 AS DOUBLE) AS bhckh275,
    CAST(bhckh276 AS DOUBLE) AS bhckh276,
    CAST(bhckh277 AS DOUBLE) AS bhckh277,
    CAST(bhckh278 AS DOUBLE) AS bhckh278,
    CAST(bhckh279 AS DOUBLE) AS bhckh279,
    CAST(bhckh280 AS DOUBLE) AS bhckh280,
    CAST(bhckh281 AS DOUBLE) AS bhckh281,
    CAST(bhckh282 AS DOUBLE) AS bhckh282,
    CAST(bhckh283 AS DOUBLE) AS bhckh283,
    CAST(bhckh284 AS DOUBLE) AS bhckh284,
    CAST(bhckh285 AS DOUBLE) AS bhckh285,
    CAST(bhckh286 AS DOUBLE) AS bhckh286,
    CAST(bhckh287 AS DOUBLE) AS bhckh287,
    CAST(bhckh288 AS DOUBLE) AS bhckh288,
    CAST(bhckh293 AS DOUBLE) AS bhckh293,
    CAST(bhckh294 AS DOUBLE) AS bhckh294,
    CAST(bhckh295 AS DOUBLE) AS bhckh295,
    CAST(bhckh296 AS DOUBLE) AS bhckh296,
    CAST(bhckh297 AS DOUBLE) AS bhckh297,
    CAST(bhckh298 AS DOUBLE) AS bhckh298,
    CAST(bhckh299 AS DOUBLE) AS bhckh299,
    CAST(bhckhj78 AS DOUBLE) AS bhckhj78,
    CAST(bhckhj79 AS DOUBLE) AS bhckhj79,
    CAST(bhckhj80 AS DOUBLE) AS bhckhj80,
    CAST(bhckhj81 AS DOUBLE) AS bhckhj81,
    CAST(bhckhj82 AS DOUBLE) AS bhckhj82,
    CAST(bhckhj83 AS DOUBLE) AS bhckhj83,
    CAST(bhckhj84 AS DOUBLE) AS bhckhj84,
    CAST(bhckhj85 AS DOUBLE) AS bhckhj85,
    CAST(bhckhj88 AS DOUBLE) AS bhckhj88,
    CAST(bhckhj89 AS DOUBLE) AS bhckhj89,
    CAST(bhckhj92 AS DOUBLE) AS bhckhj92,
    CAST(bhckhj93 AS DOUBLE) AS bhckhj93,
    CAST(bhckhj94 AS DOUBLE) AS bhckhj94,
    CAST(bhckhj95 AS DOUBLE) AS bhckhj95,
    CAST(bhckhk03 AS DOUBLE) AS bhckhk03,
    CAST(bhckhk04 AS DOUBLE) AS bhckhk04,
    CAST(bhckht58 AS DOUBLE) AS bhckht58,
    CAST(bhckht59 AS DOUBLE) AS bhckht59,
    CAST(bhckht60 AS DOUBLE) AS bhckht60,
    CAST(bhckht61 AS DOUBLE) AS bhckht61,
    CAST(bhckht62 AS DOUBLE) AS bhckht62,
    CAST(bhckht63 AS DOUBLE) AS bhckht63,
    CAST(bhckht64 AS DOUBLE) AS bhckht64,
    CAST(bhckht65 AS DOUBLE) AS bhckht65,
    CAST(bhckht69 AS DOUBLE) AS bhckht69,
    CAST(bhckht80 AS DOUBLE) AS bhckht80,
    CAST(bhckht83 AS DOUBLE) AS bhckht83,
    CAST(bhckht84 AS DOUBLE) AS bhckht84,
    CAST(bhckht85 AS DOUBLE) AS bhckht85,
    CAST(bhckht87 AS DOUBLE) AS bhckht87,
    CAST(bhckht88 AS DOUBLE) AS bhckht88,
    CAST(bhckht89 AS DOUBLE) AS bhckht89,
    CAST(bhckht91 AS DOUBLE) AS bhckht91,
    CAST(bhckht92 AS DOUBLE) AS bhckht92,
    CAST(bhckht93 AS DOUBLE) AS bhckht93,
    CAST(bhckhu09 AS DOUBLE) AS bhckhu09,
    CAST(bhckhu10 AS DOUBLE) AS bhckhu10,
    CAST(bhckhu11 AS DOUBLE) AS bhckhu11,
    CAST(bhckhu12 AS DOUBLE) AS bhckhu12,
    CAST(bhckhu13 AS DOUBLE) AS bhckhu13,
    CAST(bhckhu14 AS DOUBLE) AS bhckhu14,
    CAST(bhckhu15 AS DOUBLE) AS bhckhu15,
    CAST(bhckhu20 AS DOUBLE) AS bhckhu20,
    CAST(bhckhu21 AS DOUBLE) AS bhckhu21,
    CAST(bhckhu22 AS DOUBLE) AS bhckhu22,
    CAST(bhckhu23 AS DOUBLE) AS bhckhu23,
    CAST(bhckj320 AS DOUBLE) AS bhckj320,
    CAST(bhckj447 AS DOUBLE) AS bhckj447,
    CAST(bhckj451 AS DOUBLE) AS bhckj451,
    CAST(bhckj452 AS DOUBLE) AS bhckj452,
    CAST(bhckj453 AS DOUBLE) AS bhckj453,
    CAST(bhckj454 AS DOUBLE) AS bhckj454,
    CAST(bhckj455 AS DOUBLE) AS bhckj455,
    CAST(bhckj456 AS DOUBLE) AS bhckj456,
    CAST(bhckj461 AS DOUBLE) AS bhckj461,
    CAST(bhckj462 AS DOUBLE) AS bhckj462,
    CAST(bhckj463 AS DOUBLE) AS bhckj463,
    CAST(bhckj536 AS DOUBLE) AS bhckj536,
    CAST(bhckj537 AS DOUBLE) AS bhckj537,
    CAST(bhckj981 AS DOUBLE) AS bhckj981,
    CAST(bhckj982 AS DOUBLE) AS bhckj982,
    CAST(bhckj983 AS DOUBLE) AS bhckj983,
    CAST(bhckj984 AS DOUBLE) AS bhckj984,
    CAST(bhckj985 AS DOUBLE) AS bhckj985,
    CAST(bhckj986 AS DOUBLE) AS bhckj986,
    CAST(bhckj987 AS DOUBLE) AS bhckj987,
    CAST(bhckj988 AS DOUBLE) AS bhckj988,
    CAST(bhckj989 AS DOUBLE) AS bhckj989,
    CAST(bhckj990 AS DOUBLE) AS bhckj990,
    CAST(bhckj991 AS DOUBLE) AS bhckj991,
    CAST(bhckj992 AS DOUBLE) AS bhckj992,
    CAST(bhckj993 AS DOUBLE) AS bhckj993,
    CAST(bhckj994 AS DOUBLE) AS bhckj994,
    CAST(bhckj995 AS DOUBLE) AS bhckj995,
    CAST(bhckj996 AS DOUBLE) AS bhckj996,
    CAST(bhckj997 AS DOUBLE) AS bhckj997,
    CAST(bhckj998 AS DOUBLE) AS bhckj998,
    CAST(bhckj999 AS DOUBLE) AS bhckj999,
    CAST(bhckja21 AS DOUBLE) AS bhckja21,
    CAST(bhckja22 AS DOUBLE) AS bhckja22,
    CAST(bhckjf76 AS DOUBLE) AS bhckjf76,
    CAST(bhckjf84 AS DOUBLE) AS bhckjf84,
    CAST(bhckjf85 AS DOUBLE) AS bhckjf85,
    CAST(bhckjf86 AS DOUBLE) AS bhckjf86,
    CAST(bhckjf87 AS DOUBLE) AS bhckjf87,
    CAST(bhckjf88 AS DOUBLE) AS bhckjf88,
    CAST(bhckjf89 AS DOUBLE) AS bhckjf89,
    CAST(bhckjf90 AS DOUBLE) AS bhckjf90,
    CAST(bhckjf91 AS DOUBLE) AS bhckjf91,
    CAST(bhckjf92 AS DOUBLE) AS bhckjf92,
    CAST(bhckjf93 AS DOUBLE) AS bhckjf93,
    CAST(bhckjh88 AS DOUBLE) AS bhckjh88,
    CAST(bhckjh91 AS DOUBLE) AS bhckjh91,
    CAST(bhckjh92 AS DOUBLE) AS bhckjh92,
    CAST(bhckjh93 AS DOUBLE) AS bhckjh93,
    CAST(bhckjh94 AS DOUBLE) AS bhckjh94,
    CAST(bhckjh97 AS DOUBLE) AS bhckjh97,
    CAST(bhckjh98 AS DOUBLE) AS bhckjh98,
    CAST(bhckjh99 AS DOUBLE) AS bhckjh99,
    CAST(bhckjj00 AS DOUBLE) AS bhckjj00,
    CAST(bhckjj01 AS DOUBLE) AS bhckjj01,
    CAST(bhckjj03 AS DOUBLE) AS bhckjj03,
    CAST(bhckjj04 AS DOUBLE) AS bhckjj04,
    CAST(bhckjj05 AS DOUBLE) AS bhckjj05,
    CAST(bhckjj06 AS DOUBLE) AS bhckjj06,
    CAST(bhckjj07 AS DOUBLE) AS bhckjj07,
    CAST(bhckjj08 AS DOUBLE) AS bhckjj08,
    CAST(bhckjj09 AS DOUBLE) AS bhckjj09,
    CAST(bhckjj11 AS DOUBLE) AS bhckjj11,
    CAST(bhckjj12 AS DOUBLE) AS bhckjj12,
    CAST(bhckjj13 AS DOUBLE) AS bhckjj13,
    CAST(bhckjj14 AS DOUBLE) AS bhckjj14,
    CAST(bhckjj15 AS DOUBLE) AS bhckjj15,
    CAST(bhckjj16 AS DOUBLE) AS bhckjj16,
    CAST(bhckjj17 AS DOUBLE) AS bhckjj17,
    CAST(bhckjj18 AS DOUBLE) AS bhckjj18,
    CAST(bhckjj19 AS DOUBLE) AS bhckjj19,
    CAST(bhckjj20 AS DOUBLE) AS bhckjj20,
    CAST(bhckjj21 AS DOUBLE) AS bhckjj21,
    CAST(bhckjj23 AS DOUBLE) AS bhckjj23,
    CAST(bhckjj24 AS DOUBLE) AS bhckjj24,
    CAST(bhckjj25 AS DOUBLE) AS bhckjj25,
    CAST(bhckjj26 AS DOUBLE) AS bhckjj26,
    CAST(bhckjj27 AS DOUBLE) AS bhckjj27,
    CAST(bhckjj28 AS DOUBLE) AS bhckjj28,
    CAST(bhckjj30 AS DOUBLE) AS bhckjj30,
    CAST(bhckjj31 AS DOUBLE) AS bhckjj31,
    CAST(bhckjj32 AS DOUBLE) AS bhckjj32,
    CAST(bhckjj34 AS DOUBLE) AS bhckjj34,
    CAST(bhckk001 AS DOUBLE) AS bhckk001,
    CAST(bhckk002 AS DOUBLE) AS bhckk002,
    CAST(bhckk003 AS DOUBLE) AS bhckk003,
    CAST(bhckk004 AS DOUBLE) AS bhckk004,
    CAST(bhckk005 AS DOUBLE) AS bhckk005,
    CAST(bhckk006 AS DOUBLE) AS bhckk006,
    CAST(bhckk007 AS DOUBLE) AS bhckk007,
    CAST(bhckk008 AS DOUBLE) AS bhckk008,
    CAST(bhckk009 AS DOUBLE) AS bhckk009,
    CAST(bhckk010 AS DOUBLE) AS bhckk010,
    CAST(bhckk011 AS DOUBLE) AS bhckk011,
    CAST(bhckk012 AS DOUBLE) AS bhckk012,
    CAST(bhckk013 AS DOUBLE) AS bhckk013,
    CAST(bhckk014 AS DOUBLE) AS bhckk014,
    CAST(bhckk015 AS DOUBLE) AS bhckk015,
    CAST(bhckk016 AS DOUBLE) AS bhckk016,
    CAST(bhckk017 AS DOUBLE) AS bhckk017,
    CAST(bhckk018 AS DOUBLE) AS bhckk018,
    CAST(bhckk019 AS DOUBLE) AS bhckk019,
    CAST(bhckk020 AS DOUBLE) AS bhckk020,
    CAST(bhckk021 AS DOUBLE) AS bhckk021,
    CAST(bhckk022 AS DOUBLE) AS bhckk022,
    CAST(bhckk023 AS DOUBLE) AS bhckk023,
    CAST(bhckk024 AS DOUBLE) AS bhckk024,
    CAST(bhckk025 AS DOUBLE) AS bhckk025,
    CAST(bhckk026 AS DOUBLE) AS bhckk026,
    CAST(bhckk027 AS DOUBLE) AS bhckk027,
    CAST(bhckk028 AS DOUBLE) AS bhckk028,
    CAST(bhckk029 AS DOUBLE) AS bhckk029,
    CAST(bhckk030 AS DOUBLE) AS bhckk030,
    CAST(bhckk031 AS DOUBLE) AS bhckk031,
    CAST(bhckk032 AS DOUBLE) AS bhckk032,
    CAST(bhckk033 AS DOUBLE) AS bhckk033,
    CAST(bhckk034 AS DOUBLE) AS bhckk034,
    CAST(bhckk035 AS DOUBLE) AS bhckk035,
    CAST(bhckk036 AS DOUBLE) AS bhckk036,
    CAST(bhckk037 AS DOUBLE) AS bhckk037,
    CAST(bhckk038 AS DOUBLE) AS bhckk038,
    CAST(bhckk039 AS DOUBLE) AS bhckk039,
    CAST(bhckk040 AS DOUBLE) AS bhckk040,
    CAST(bhckk041 AS DOUBLE) AS bhckk041,
    CAST(bhckk072 AS DOUBLE) AS bhckk072,
    CAST(bhckk073 AS DOUBLE) AS bhckk073,
    CAST(bhckk074 AS DOUBLE) AS bhckk074,
    CAST(bhckk075 AS DOUBLE) AS bhckk075,
    CAST(bhckk076 AS DOUBLE) AS bhckk076,
    CAST(bhckk077 AS DOUBLE) AS bhckk077,
    CAST(bhckk078 AS DOUBLE) AS bhckk078,
    CAST(bhckk079 AS DOUBLE) AS bhckk079,
    CAST(bhckk080 AS DOUBLE) AS bhckk080,
    CAST(bhckk081 AS DOUBLE) AS bhckk081,
    CAST(bhckk082 AS DOUBLE) AS bhckk082,
    CAST(bhckk083 AS DOUBLE) AS bhckk083,
    CAST(bhckk084 AS DOUBLE) AS bhckk084,
    CAST(bhckk085 AS DOUBLE) AS bhckk085,
    CAST(bhckk086 AS DOUBLE) AS bhckk086,
    CAST(bhckk087 AS DOUBLE) AS bhckk087,
    CAST(bhckk088 AS DOUBLE) AS bhckk088,
    CAST(bhckk089 AS DOUBLE) AS bhckk089,
    CAST(bhckk090 AS DOUBLE) AS bhckk090,
    CAST(bhckk091 AS DOUBLE) AS bhckk091,
    CAST(bhckk092 AS DOUBLE) AS bhckk092,
    CAST(bhckk093 AS DOUBLE) AS bhckk093,
    CAST(bhckk094 AS DOUBLE) AS bhckk094,
    CAST(bhckk095 AS DOUBLE) AS bhckk095,
    CAST(bhckk096 AS DOUBLE) AS bhckk096,
    CAST(bhckk097 AS DOUBLE) AS bhckk097,
    CAST(bhckk098 AS DOUBLE) AS bhckk098,
    CAST(bhckk099 AS DOUBLE) AS bhckk099,
    CAST(bhckk100 AS DOUBLE) AS bhckk100,
    CAST(bhckk101 AS DOUBLE) AS bhckk101,
    CAST(bhckk120 AS DOUBLE) AS bhckk120,
    CAST(bhckk121 AS DOUBLE) AS bhckk121,
    CAST(bhckk122 AS DOUBLE) AS bhckk122,
    CAST(bhckk123 AS DOUBLE) AS bhckk123,
    CAST(bhckk124 AS DOUBLE) AS bhckk124,
    CAST(bhckk125 AS DOUBLE) AS bhckk125,
    CAST(bhckk126 AS DOUBLE) AS bhckk126,
    CAST(bhckk127 AS DOUBLE) AS bhckk127,
    CAST(bhckk128 AS DOUBLE) AS bhckk128,
    CAST(bhckk129 AS DOUBLE) AS bhckk129,
    CAST(bhckk134 AS DOUBLE) AS bhckk134,
    CAST(bhckk135 AS DOUBLE) AS bhckk135,
    CAST(bhckk136 AS DOUBLE) AS bhckk136,
    CAST(bhckk137 AS DOUBLE) AS bhckk137,
    CAST(bhckk138 AS DOUBLE) AS bhckk138,
    CAST(bhckk139 AS DOUBLE) AS bhckk139,
    CAST(bhckk140 AS DOUBLE) AS bhckk140,
    CAST(bhckk142 AS DOUBLE) AS bhckk142,
    CAST(bhckk143 AS DOUBLE) AS bhckk143,
    CAST(bhckk144 AS DOUBLE) AS bhckk144,
    CAST(bhckk145 AS DOUBLE) AS bhckk145,
    CAST(bhckk146 AS DOUBLE) AS bhckk146,
    CAST(bhckk147 AS DOUBLE) AS bhckk147,
    CAST(bhckk148 AS DOUBLE) AS bhckk148,
    CAST(bhckk149 AS DOUBLE) AS bhckk149,
    CAST(bhckk150 AS DOUBLE) AS bhckk150,
    CAST(bhckk151 AS DOUBLE) AS bhckk151,
    CAST(bhckk152 AS DOUBLE) AS bhckk152,
    CAST(bhckk153 AS DOUBLE) AS bhckk153,
    CAST(bhckk154 AS DOUBLE) AS bhckk154,
    CAST(bhckk155 AS DOUBLE) AS bhckk155,
    CAST(bhckk156 AS DOUBLE) AS bhckk156,
    CAST(bhckk157 AS DOUBLE) AS bhckk157,
    CAST(bhckk163 AS DOUBLE) AS bhckk163,
    CAST(bhckk164 AS DOUBLE) AS bhckk164,
    CAST(bhckk165 AS DOUBLE) AS bhckk165,
    CAST(bhckk167 AS DOUBLE) AS bhckk167,
    CAST(bhckk168 AS DOUBLE) AS bhckk168,
    CAST(bhckk178 AS DOUBLE) AS bhckk178,
    CAST(bhckk179 AS DOUBLE) AS bhckk179,
    CAST(bhckk180 AS DOUBLE) AS bhckk180,
    CAST(bhckk181 AS DOUBLE) AS bhckk181,
    CAST(bhckk182 AS DOUBLE) AS bhckk182,
    CAST(bhckk183 AS DOUBLE) AS bhckk183,
    CAST(bhckk184 AS DOUBLE) AS bhckk184,
    CAST(bhckk185 AS DOUBLE) AS bhckk185,
    CAST(bhckk186 AS DOUBLE) AS bhckk186,
    CAST(bhckk192 AS DOUBLE) AS bhckk192,
    CAST(bhckk193 AS DOUBLE) AS bhckk193,
    CAST(bhckk194 AS DOUBLE) AS bhckk194,
    CAST(bhckk196 AS DOUBLE) AS bhckk196,
    CAST(bhckk201 AS DOUBLE) AS bhckk201,
    CAST(bhckk202 AS DOUBLE) AS bhckk202,
    CAST(bhckk203 AS DOUBLE) AS bhckk203,
    CAST(bhckk204 AS DOUBLE) AS bhckk204,
    CAST(bhckk205 AS DOUBLE) AS bhckk205,
    CAST(bhckk207 AS DOUBLE) AS bhckk207,
    CAST(bhckk208 AS DOUBLE) AS bhckk208,
    CAST(bhckk212 AS DOUBLE) AS bhckk212,
    CAST(bhckk213 AS DOUBLE) AS bhckk213,
    CAST(bhckk214 AS DOUBLE) AS bhckk214,
    CAST(bhckk215 AS DOUBLE) AS bhckk215,
    CAST(bhckk216 AS DOUBLE) AS bhckk216,
    CAST(bhckk217 AS DOUBLE) AS bhckk217,
    CAST(bhckk218 AS DOUBLE) AS bhckk218,
    CAST(bhckk267 AS DOUBLE) AS bhckk267,
    CAST(bhckk269 AS DOUBLE) AS bhckk269,
    CAST(bhckk270 AS DOUBLE) AS bhckk270,
    CAST(bhckk271 AS DOUBLE) AS bhckk271,
    CAST(bhckk272 AS DOUBLE) AS bhckk272,
    CAST(bhckk273 AS DOUBLE) AS bhckk273,
    CAST(bhckk274 AS DOUBLE) AS bhckk274,
    CAST(bhckk275 AS DOUBLE) AS bhckk275,
    CAST(bhckk276 AS DOUBLE) AS bhckk276,
    CAST(bhckk277 AS DOUBLE) AS bhckk277,
    CAST(bhckk278 AS DOUBLE) AS bhckk278,
    CAST(bhckk279 AS DOUBLE) AS bhckk279,
    CAST(bhckk280 AS DOUBLE) AS bhckk280,
    CAST(bhckk281 AS DOUBLE) AS bhckk281,
    CAST(bhckk282 AS DOUBLE) AS bhckk282,
    CAST(bhckk283 AS DOUBLE) AS bhckk283,
    CAST(bhckk284 AS DOUBLE) AS bhckk284,
    CAST(bhckk285 AS DOUBLE) AS bhckk285,
    CAST(bhckk286 AS DOUBLE) AS bhckk286,
    CAST(bhckk287 AS DOUBLE) AS bhckk287,
    CAST(bhckk288 AS DOUBLE) AS bhckk288,
    CAST(bhckkx46 AS DOUBLE) AS bhckkx46,
    CAST(bhckkx47 AS DOUBLE) AS bhckkx47,
    CAST(bhckkx50 AS DOUBLE) AS bhckkx50,
    CAST(bhckkx51 AS DOUBLE) AS bhckkx51,
    CAST(bhckkx52 AS DOUBLE) AS bhckkx52,
    CAST(bhckkx53 AS DOUBLE) AS bhckkx53,
    CAST(bhckkx54 AS DOUBLE) AS bhckkx54,
    CAST(bhckkx55 AS DOUBLE) AS bhckkx55,
    CAST(bhckkx57 AS DOUBLE) AS bhckkx57,
    CAST(bhckkx58 AS DOUBLE) AS bhckkx58,
    CAST(bhckkx60 AS DOUBLE) AS bhckkx60,
    CAST(bhckkx61 AS DOUBLE) AS bhckkx61,
    CAST(bhckkx62 AS DOUBLE) AS bhckkx62,
    CAST(bhckkx63 AS DOUBLE) AS bhckkx63,
    CAST(bhckkx64 AS DOUBLE) AS bhckkx64,
    CAST(bhckkx65 AS DOUBLE) AS bhckkx65,
    CAST(bhckky38 AS BOOLEAN) AS bhckky38,
    CAST(bhcklg24 AS BOOLEAN) AS bhcklg24,
    CAST(bhcklg26 AS DOUBLE) AS bhcklg26,
    CAST(bhckm727 AS DOUBLE) AS bhckm727,
    CAST(bhckm728 AS DOUBLE) AS bhckm728,
    CAST(bhckm729 AS DOUBLE) AS bhckm729,
    CAST(bhckm730 AS DOUBLE) AS bhckm730,
    CAST(bhckm731 AS DOUBLE) AS bhckm731,
    CAST(bhckm732 AS DOUBLE) AS bhckm732,
    CAST(bhckm733 AS DOUBLE) AS bhckm733,
    CAST(bhckm734 AS DOUBLE) AS bhckm734,
    CAST(bhckm735 AS DOUBLE) AS bhckm735,
    CAST(bhckm736 AS DOUBLE) AS bhckm736,
    CAST(bhckm737 AS DOUBLE) AS bhckm737,
    CAST(bhckm738 AS DOUBLE) AS bhckm738,
    CAST(bhckm739 AS DOUBLE) AS bhckm739,
    CAST(bhckm740 AS DOUBLE) AS bhckm740,
    CAST(bhckm741 AS DOUBLE) AS bhckm741,
    CAST(bhckm742 AS DOUBLE) AS bhckm742,
    CAST(bhckm743 AS DOUBLE) AS bhckm743,
    CAST(bhckm744 AS DOUBLE) AS bhckm744,
    CAST(bhckm962 AS DOUBLE) AS bhckm962,
    CAST(bhckmg94 AS DOUBLE) AS bhckmg94,
    CAST(bhcks396 AS DOUBLE) AS bhcks396,
    CAST(bhcks397 AS DOUBLE) AS bhcks397,
    CAST(bhcks398 AS DOUBLE) AS bhcks398,
    CAST(bhcks399 AS DOUBLE) AS bhcks399,
    CAST(bhcks400 AS DOUBLE) AS bhcks400,
    CAST(bhcks402 AS DOUBLE) AS bhcks402,
    CAST(bhcks403 AS DOUBLE) AS bhcks403,
    CAST(bhcks405 AS DOUBLE) AS bhcks405,
    CAST(bhcks406 AS DOUBLE) AS bhcks406,
    CAST(bhcks410 AS DOUBLE) AS bhcks410,
    CAST(bhcks411 AS DOUBLE) AS bhcks411,
    CAST(bhcks414 AS DOUBLE) AS bhcks414,
    CAST(bhcks415 AS DOUBLE) AS bhcks415,
    CAST(bhcks416 AS DOUBLE) AS bhcks416,
    CAST(bhcks417 AS DOUBLE) AS bhcks417,
    CAST(bhcks420 AS DOUBLE) AS bhcks420,
    CAST(bhcks421 AS DOUBLE) AS bhcks421,
    CAST(bhcks424 AS DOUBLE) AS bhcks424,
    CAST(bhcks425 AS DOUBLE) AS bhcks425,
    CAST(bhcks426 AS DOUBLE) AS bhcks426,
    CAST(bhcks427 AS DOUBLE) AS bhcks427,
    CAST(bhcks428 AS DOUBLE) AS bhcks428,
    CAST(bhcks429 AS DOUBLE) AS bhcks429,
    CAST(bhcks432 AS DOUBLE) AS bhcks432,
    CAST(bhcks433 AS DOUBLE) AS bhcks433,
    CAST(bhcks434 AS DOUBLE) AS bhcks434,
    CAST(bhcks435 AS DOUBLE) AS bhcks435,
    CAST(bhcks436 AS DOUBLE) AS bhcks436,
    CAST(bhcks437 AS DOUBLE) AS bhcks437,
    CAST(bhcks440 AS DOUBLE) AS bhcks440,
    CAST(bhcks441 AS DOUBLE) AS bhcks441,
    CAST(bhcks442 AS DOUBLE) AS bhcks442,
    CAST(bhcks443 AS DOUBLE) AS bhcks443,
    CAST(bhcks446 AS DOUBLE) AS bhcks446,
    CAST(bhcks447 AS DOUBLE) AS bhcks447,
    CAST(bhcks450 AS DOUBLE) AS bhcks450,
    CAST(bhcks451 AS DOUBLE) AS bhcks451,
    CAST(bhcks452 AS DOUBLE) AS bhcks452,
    CAST(bhcks453 AS DOUBLE) AS bhcks453,
    CAST(bhcks454 AS DOUBLE) AS bhcks454,
    CAST(bhcks455 AS DOUBLE) AS bhcks455,
    CAST(bhcks458 AS DOUBLE) AS bhcks458,
    CAST(bhcks459 AS DOUBLE) AS bhcks459,
    CAST(bhcks460 AS DOUBLE) AS bhcks460,
    CAST(bhcks461 AS DOUBLE) AS bhcks461,
    CAST(bhcks462 AS DOUBLE) AS bhcks462,
    CAST(bhcks463 AS DOUBLE) AS bhcks463,
    CAST(bhcks469 AS DOUBLE) AS bhcks469,
    CAST(bhcks470 AS DOUBLE) AS bhcks470,
    CAST(bhcks471 AS DOUBLE) AS bhcks471,
    CAST(bhcks476 AS DOUBLE) AS bhcks476,
    CAST(bhcks477 AS DOUBLE) AS bhcks477,
    CAST(bhcks478 AS DOUBLE) AS bhcks478,
    CAST(bhcks479 AS DOUBLE) AS bhcks479,
    CAST(bhcks481 AS DOUBLE) AS bhcks481,
    CAST(bhcks482 AS DOUBLE) AS bhcks482,
    CAST(bhcks483 AS DOUBLE) AS bhcks483,
    CAST(bhcks484 AS DOUBLE) AS bhcks484,
    CAST(bhcks486 AS DOUBLE) AS bhcks486,
    CAST(bhcks487 AS DOUBLE) AS bhcks487,
    CAST(bhcks488 AS DOUBLE) AS bhcks488,
    CAST(bhcks489 AS DOUBLE) AS bhcks489,
    CAST(bhcks491 AS DOUBLE) AS bhcks491,
    CAST(bhcks492 AS DOUBLE) AS bhcks492,
    CAST(bhcks493 AS DOUBLE) AS bhcks493,
    CAST(bhcks494 AS DOUBLE) AS bhcks494,
    CAST(bhcks496 AS DOUBLE) AS bhcks496,
    CAST(bhcks497 AS DOUBLE) AS bhcks497,
    CAST(bhcks498 AS DOUBLE) AS bhcks498,
    CAST(bhcks499 AS DOUBLE) AS bhcks499,
    CAST(bhcks511 AS DOUBLE) AS bhcks511,
    CAST(bhcks513 AS DOUBLE) AS bhcks513,
    CAST(bhcks524 AS DOUBLE) AS bhcks524,
    CAST(bhcks549 AS DOUBLE) AS bhcks549,
    CAST(bhcks550 AS DOUBLE) AS bhcks550,
    CAST(bhcks551 AS DOUBLE) AS bhcks551,
    CAST(bhcks552 AS DOUBLE) AS bhcks552,
    CAST(bhcks554 AS DOUBLE) AS bhcks554,
    CAST(bhcks555 AS DOUBLE) AS bhcks555,
    CAST(bhcks556 AS DOUBLE) AS bhcks556,
    CAST(bhcks557 AS DOUBLE) AS bhcks557,
    CAST(bhcks582 AS DOUBLE) AS bhcks582,
    CAST(bhcks583 AS DOUBLE) AS bhcks583,
    CAST(bhcks584 AS DOUBLE) AS bhcks584,
    CAST(bhcks585 AS DOUBLE) AS bhcks585,
    CAST(bhcks586 AS DOUBLE) AS bhcks586,
    CAST(bhcks587 AS DOUBLE) AS bhcks587,
    CAST(bhcks588 AS DOUBLE) AS bhcks588,
    CAST(bhcks589 AS DOUBLE) AS bhcks589,
    CAST(bhcks590 AS DOUBLE) AS bhcks590,
    CAST(bhcks591 AS DOUBLE) AS bhcks591,
    CAST(bhcks592 AS DOUBLE) AS bhcks592,
    CAST(bhcks593 AS DOUBLE) AS bhcks593,
    CAST(bhcks594 AS DOUBLE) AS bhcks594,
    CAST(bhcks595 AS DOUBLE) AS bhcks595,
    CAST(bhcks596 AS DOUBLE) AS bhcks596,
    CAST(bhcks597 AS DOUBLE) AS bhcks597,
    CAST(bhcks598 AS DOUBLE) AS bhcks598,
    CAST(bhcks599 AS DOUBLE) AS bhcks599,
    CAST(bhcks600 AS DOUBLE) AS bhcks600,
    CAST(bhcks601 AS DOUBLE) AS bhcks601,
    CAST(bhcks602 AS DOUBLE) AS bhcks602,
    CAST(bhcks603 AS DOUBLE) AS bhcks603,
    CAST(bhcks604 AS DOUBLE) AS bhcks604,
    CAST(bhcks605 AS DOUBLE) AS bhcks605,
    CAST(bhcks606 AS DOUBLE) AS bhcks606,
    CAST(bhcks607 AS DOUBLE) AS bhcks607,
    CAST(bhcks608 AS DOUBLE) AS bhcks608,
    CAST(bhcks609 AS DOUBLE) AS bhcks609,
    CAST(bhcks610 AS DOUBLE) AS bhcks610,
    CAST(bhcks611 AS DOUBLE) AS bhcks611,
    CAST(bhcks612 AS DOUBLE) AS bhcks612,
    CAST(bhcks613 AS DOUBLE) AS bhcks613,
    CAST(bhcks614 AS DOUBLE) AS bhcks614,
    CAST(bhcks615 AS DOUBLE) AS bhcks615,
    CAST(bhcks616 AS DOUBLE) AS bhcks616,
    CAST(bhcks617 AS DOUBLE) AS bhcks617,
    CAST(bhcks618 AS DOUBLE) AS bhcks618,
    CAST(bhcks619 AS DOUBLE) AS bhcks619,
    CAST(bhcks620 AS DOUBLE) AS bhcks620,
    CAST(bhcks621 AS DOUBLE) AS bhcks621,
    CAST(bhcks622 AS DOUBLE) AS bhcks622,
    CAST(bhcks623 AS DOUBLE) AS bhcks623,
    CAST(bhckt047 AS DOUBLE) AS bhckt047,
    CAST(bhcky923 AS DOUBLE) AS bhcky923,
    CAST(bhcky924 AS DOUBLE) AS bhcky924,
    TRY_CAST(rssd9001 AS DOUBLE) AS rssd9001,
    CAST(rssd9017 AS VARCHAR) AS rssd9017,
    TRY_CAST(strftime(CAST(rssd9999 AS DATE), '%Y%m%d') AS DOUBLE) AS rssd9999,
    CAST(wrdsdownloaddate AS DATE) AS wrdsdownloaddate
FROM {}
WHERE CAST(rssd9999 AS DATE) BETWEEN DATE '{}' AND DATE '{}' 
ORDER BY rssd9001, rssd9999"#,
                table,
                date_range.0.to_string(),
                date_range.1.to_string()
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            let mut stmt = conn_guard.prepare(sql.as_str())?;
            let mut reader = stmt.query_arrow([])?;
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();
                let s = |name: &str| -> &StringArray {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                };
                let f = |name: &str| -> &Float64Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                };
                let b = |name: &str| -> &BooleanArray {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                };
                let d = |name: &str| -> &Date32Array {
                    batch
                        .column(schema.index_of(name).unwrap())
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                };
                // Pre-bind arrays
                let bhck0010 = f("bhck0010");
                let bhck0081 = f("bhck0081");
                let bhck0211 = f("bhck0211");
                let bhck0213 = f("bhck0213");
                let bhck0379 = f("bhck0379");
                let bhck0395 = f("bhck0395");
                let bhck0397 = f("bhck0397");
                let bhck0426 = f("bhck0426");
                let bhck0497 = f("bhck0497");
                let bhck1226 = f("bhck1226");
                let bhck1227 = f("bhck1227");
                let bhck1228 = f("bhck1228");
                let bhck1286 = f("bhck1286");
                let bhck1287 = f("bhck1287");
                let bhck1288 = f("bhck1288");
                let bhck1289 = f("bhck1289");
                let bhck1290 = f("bhck1290");
                let bhck1291 = f("bhck1291");
                let bhck1292 = f("bhck1292");
                let bhck1293 = f("bhck1293");
                let bhck1294 = f("bhck1294");
                let bhck1295 = f("bhck1295");
                let bhck1296 = f("bhck1296");
                let bhck1297 = f("bhck1297");
                let bhck1298 = f("bhck1298");
                let bhck1350 = f("bhck1350");
                let bhck1410 = f("bhck1410");
                let bhck1421 = b("bhck1421");
                let bhck1422 = f("bhck1422");
                let bhck1423 = f("bhck1423");
                let bhck1545 = f("bhck1545");
                let bhck1563 = f("bhck1563");
                let bhck1564 = f("bhck1564");
                let bhck1583 = f("bhck1583");
                let bhck1590 = f("bhck1590");
                let bhck1594 = f("bhck1594");
                let bhck1597 = f("bhck1597");
                let bhck1606 = f("bhck1606");
                let bhck1607 = f("bhck1607");
                let bhck1608 = f("bhck1608");
                let bhck1611 = f("bhck1611");
                let bhck1612 = f("bhck1612");
                let bhck1613 = f("bhck1613");
                let bhck1615 = f("bhck1615");
                let bhck1616 = f("bhck1616");
                let bhck1635 = f("bhck1635");
                let bhck1636 = f("bhck1636");
                let bhck1638 = f("bhck1638");
                let bhck1639 = f("bhck1639");
                let bhck1651 = f("bhck1651");
                let bhck1698 = f("bhck1698");
                let bhck1699 = f("bhck1699");
                let bhck1701 = f("bhck1701");
                let bhck1702 = f("bhck1702");
                let bhck1703 = f("bhck1703");
                let bhck1705 = f("bhck1705");
                let bhck1706 = f("bhck1706");
                let bhck1707 = f("bhck1707");
                let bhck1709 = f("bhck1709");
                let bhck1710 = f("bhck1710");
                let bhck1711 = f("bhck1711");
                let bhck1713 = f("bhck1713");
                let bhck1714 = f("bhck1714");
                let bhck1715 = f("bhck1715");
                let bhck1716 = f("bhck1716");
                let bhck1717 = f("bhck1717");
                let bhck1718 = f("bhck1718");
                let bhck1719 = f("bhck1719");
                let bhck1727 = f("bhck1727");
                let bhck1731 = f("bhck1731");
                let bhck1732 = f("bhck1732");
                let bhck1733 = f("bhck1733");
                let bhck1734 = f("bhck1734");
                let bhck1735 = f("bhck1735");
                let bhck1736 = f("bhck1736");
                let bhck1737 = f("bhck1737");
                let bhck1738 = f("bhck1738");
                let bhck1739 = f("bhck1739");
                let bhck1741 = f("bhck1741");
                let bhck1742 = f("bhck1742");
                let bhck1743 = f("bhck1743");
                let bhck1744 = f("bhck1744");
                let bhck1746 = f("bhck1746");
                let bhck1752 = f("bhck1752");
                let bhck1753 = f("bhck1753");
                let bhck1754 = f("bhck1754");
                let bhck1755 = f("bhck1755");
                let bhck1763 = f("bhck1763");
                let bhck1764 = f("bhck1764");
                let bhck1766 = f("bhck1766");
                let bhck1773 = f("bhck1773");
                let bhck1778 = f("bhck1778");
                let bhck1912 = f("bhck1912");
                let bhck1913 = f("bhck1913");
                let bhck1975 = f("bhck1975");
                let bhck2008 = f("bhck2008");
                let bhck2011 = f("bhck2011");
                let bhck2081 = f("bhck2081");
                let bhck2130 = f("bhck2130");
                let bhck2143 = f("bhck2143");
                let bhck2148 = f("bhck2148");
                let bhck2150 = f("bhck2150");
                let bhck2155 = f("bhck2155");
                let bhck2160 = f("bhck2160");
                let bhck2165 = f("bhck2165");
                let bhck2168 = f("bhck2168");
                let bhck2182 = f("bhck2182");
                let bhck2183 = f("bhck2183");
                let bhck2309 = f("bhck2309");
                let bhck2332 = f("bhck2332");
                let bhck2333 = f("bhck2333");
                let bhck2745 = f("bhck2745");
                let bhck2746 = f("bhck2746");
                let bhck2747 = f("bhck2747");
                let bhck2748 = f("bhck2748");
                let bhck2749 = f("bhck2749");
                let bhck2750 = f("bhck2750");
                let bhck2757 = f("bhck2757");
                let bhck2759 = f("bhck2759");
                let bhck2769 = f("bhck2769");
                let bhck2771 = f("bhck2771");
                let bhck2800 = f("bhck2800");
                let bhck2920 = f("bhck2920");
                let bhck3000 = f("bhck3000");
                let bhck3049 = f("bhck3049");
                let bhck3123 = f("bhck3123");
                let bhck3124 = f("bhck3124");
                let bhck3128 = f("bhck3128");
                let bhck3153 = f("bhck3153");
                let bhck3163 = f("bhck3163");
                let bhck3164 = f("bhck3164");
                let bhck3190 = f("bhck3190");
                let bhck3197 = f("bhck3197");
                let bhck3215 = f("bhck3215");
                let bhck3216 = f("bhck3216");
                let bhck3217 = f("bhck3217");
                let bhck3230 = f("bhck3230");
                let bhck3284 = f("bhck3284");
                let bhck3296 = f("bhck3296");
                let bhck3297 = f("bhck3297");
                let bhck3298 = f("bhck3298");
                let bhck3409 = f("bhck3409");
                let bhck3411 = f("bhck3411");
                let bhck3430 = f("bhck3430");
                let bhck3434 = f("bhck3434");
                let bhck3435 = f("bhck3435");
                let bhck3450 = f("bhck3450");
                let bhck3451 = b("bhck3451");
                let bhck3452 = b("bhck3452");
                let bhck3492 = f("bhck3492");
                let bhck3493 = f("bhck3493");
                let bhck3494 = f("bhck3494");
                let bhck3495 = f("bhck3495");
                let bhck3499 = f("bhck3499");
                let bhck3500 = f("bhck3500");
                let bhck3501 = f("bhck3501");
                let bhck3502 = f("bhck3502");
                let bhck3503 = f("bhck3503");
                let bhck3504 = f("bhck3504");
                let bhck3505 = f("bhck3505");
                let bhck3506 = f("bhck3506");
                let bhck3507 = f("bhck3507");
                let bhck3508 = f("bhck3508");
                let bhck3522 = b("bhck3522");
                let bhck3528 = f("bhck3528");
                let bhck3529 = f("bhck3529");
                let bhck3530 = f("bhck3530");
                let bhck3541 = f("bhck3541");
                let bhck3546 = f("bhck3546");
                let bhck3571 = f("bhck3571");
                let bhck3572 = f("bhck3572");
                let bhck3574 = f("bhck3574");
                let bhck3576 = f("bhck3576");
                let bhck3578 = f("bhck3578");
                let bhck3580 = f("bhck3580");
                let bhck3581 = f("bhck3581");
                let bhck3582 = f("bhck3582");
                let bhck3584 = f("bhck3584");
                let bhck3588 = f("bhck3588");
                let bhck3590 = f("bhck3590");
                let bhck3656 = f("bhck3656");
                let bhck3806 = f("bhck3806");
                let bhck3809 = f("bhck3809");
                let bhck3812 = f("bhck3812");
                let bhck3816 = f("bhck3816");
                let bhck3820 = f("bhck3820");
                let bhck3822 = f("bhck3822");
                let bhck3826 = f("bhck3826");
                let bhck3836 = f("bhck3836");
                let bhck3837 = f("bhck3837");
                let bhck4010 = f("bhck4010");
                let bhck4020 = f("bhck4020");
                let bhck4027 = f("bhck4027");
                let bhck4042 = f("bhck4042");
                let bhck4059 = f("bhck4059");
                let bhck4060 = f("bhck4060");
                let bhck4065 = f("bhck4065");
                let bhck4069 = f("bhck4069");
                let bhck4070 = f("bhck4070");
                let bhck4074 = f("bhck4074");
                let bhck4078 = f("bhck4078");
                let bhck4092 = f("bhck4092");
                let bhck4105 = f("bhck4105");
                let bhck4106 = f("bhck4106");
                let bhck4115 = f("bhck4115");
                let bhck4136 = f("bhck4136");
                let bhck4141 = f("bhck4141");
                let bhck4146 = f("bhck4146");
                let bhck4150 = f("bhck4150");
                let bhck4172 = f("bhck4172");
                let bhck4180 = f("bhck4180");
                let bhck4185 = f("bhck4185");
                let bhck4217 = f("bhck4217");
                let bhck4219 = f("bhck4219");
                let bhck4300 = f("bhck4300");
                let bhck4301 = f("bhck4301");
                let bhck4302 = f("bhck4302");
                let bhck4313 = f("bhck4313");
                let bhck4320 = f("bhck4320");
                let bhck4336 = f("bhck4336");
                let bhck4340 = f("bhck4340");
                let bhck4356 = f("bhck4356");
                let bhck4393 = f("bhck4393");
                let bhck4394 = f("bhck4394");
                let bhck4395 = f("bhck4395");
                let bhck4396 = f("bhck4396");
                let bhck4397 = f("bhck4397");
                let bhck4398 = f("bhck4398");
                let bhck4399 = f("bhck4399");
                let bhck4411 = f("bhck4411");
                let bhck4412 = f("bhck4412");
                let bhck4414 = f("bhck4414");
                let bhck4435 = f("bhck4435");
                let bhck4436 = f("bhck4436");
                let bhck4460 = f("bhck4460");
                let bhck4484 = f("bhck4484");
                let bhck4503 = f("bhck4503");
                let bhck4504 = f("bhck4504");
                let bhck4506 = f("bhck4506");
                let bhck4507 = f("bhck4507");
                let bhck4518 = f("bhck4518");
                let bhck4519 = f("bhck4519");
                let bhck4531 = f("bhck4531");
                let bhck4574 = f("bhck4574");
                let bhck4591 = f("bhck4591");
                let bhck4592 = f("bhck4592");
                let bhck4598 = f("bhck4598");
                let bhck4635 = f("bhck4635");
                let bhck4643 = f("bhck4643");
                let bhck4644 = f("bhck4644");
                let bhck4645 = f("bhck4645");
                let bhck4646 = f("bhck4646");
                let bhck4651 = f("bhck4651");
                let bhck4652 = f("bhck4652");
                let bhck4653 = f("bhck4653");
                let bhck4654 = f("bhck4654");
                let bhck4655 = f("bhck4655");
                let bhck4656 = f("bhck4656");
                let bhck4657 = f("bhck4657");
                let bhck4658 = f("bhck4658");
                let bhck4659 = f("bhck4659");
                let bhck4776 = f("bhck4776");
                let bhck4815 = f("bhck4815");
                let bhck4832 = f("bhck4832");
                let bhck4833 = f("bhck4833");
                let bhck4834 = f("bhck4834");
                let bhck5041 = f("bhck5041");
                let bhck5043 = f("bhck5043");
                let bhck5045 = f("bhck5045");
                let bhck5047 = f("bhck5047");
                let bhck5310 = f("bhck5310");
                let bhck5351 = f("bhck5351");
                let bhck5354 = f("bhck5354");
                let bhck5355 = f("bhck5355");
                let bhck5356 = f("bhck5356");
                let bhck5359 = f("bhck5359");
                let bhck5360 = f("bhck5360");
                let bhck5369 = f("bhck5369");
                let bhck5377 = f("bhck5377");
                let bhck5378 = f("bhck5378");
                let bhck5379 = f("bhck5379");
                let bhck5380 = f("bhck5380");
                let bhck5381 = f("bhck5381");
                let bhck5382 = f("bhck5382");
                let bhck5383 = b("bhck5383");
                let bhck5384 = f("bhck5384");
                let bhck5385 = f("bhck5385");
                let bhck5386 = b("bhck5386");
                let bhck5387 = f("bhck5387");
                let bhck5388 = f("bhck5388");
                let bhck5389 = f("bhck5389");
                let bhck5390 = f("bhck5390");
                let bhck5391 = f("bhck5391");
                let bhck5393 = f("bhck5393");
                let bhck5397 = f("bhck5397");
                let bhck5398 = f("bhck5398");
                let bhck5399 = f("bhck5399");
                let bhck5400 = f("bhck5400");
                let bhck5401 = f("bhck5401");
                let bhck5402 = f("bhck5402");
                let bhck5403 = f("bhck5403");
                let bhck5409 = f("bhck5409");
                let bhck5411 = f("bhck5411");
                let bhck5413 = f("bhck5413");
                let bhck5459 = f("bhck5459");
                let bhck5460 = f("bhck5460");
                let bhck5461 = f("bhck5461");
                let bhck5507 = f("bhck5507");
                let bhck5610 = f("bhck5610");
                let bhck5612 = f("bhck5612");
                let bhck5613 = f("bhck5613");
                let bhck5614 = f("bhck5614");
                let bhck5615 = f("bhck5615");
                let bhck5616 = f("bhck5616");
                let bhck5617 = f("bhck5617");
                let bhck6019 = f("bhck6019");
                let bhck6373 = f("bhck6373");
                let bhck6416 = f("bhck6416");
                let bhck6438 = f("bhck6438");
                let bhck6441 = f("bhck6441");
                let bhck6442 = f("bhck6442");
                let bhck6550 = f("bhck6550");
                let bhck6555 = f("bhck6555");
                let bhck6556 = f("bhck6556");
                let bhck6557 = f("bhck6557");
                let bhck6558 = f("bhck6558");
                let bhck6559 = f("bhck6559");
                let bhck6560 = f("bhck6560");
                let bhck6561 = f("bhck6561");
                let bhck6566 = f("bhck6566");
                let bhck6572 = f("bhck6572");
                let bhck6586 = f("bhck6586");
                let bhck6599 = f("bhck6599");
                let bhck6649 = f("bhck6649");
                let bhck6669 = b("bhck6669");
                let bhck6688 = f("bhck6688");
                let bhck6689 = f("bhck6689");
                let bhck6761 = f("bhck6761");
                let bhck6765 = f("bhck6765");
                let bhck6927 = b("bhck6927");
                let bhck6928 = b("bhck6928");
                let bhck6995 = b("bhck6995");
                let bhck6998 = b("bhck6998");
                let bhck8403 = f("bhck8403");
                let bhck8427 = f("bhck8427");
                let bhck8428 = f("bhck8428");
                let bhck8429 = f("bhck8429");
                let bhck8430 = f("bhck8430");
                let bhck8431 = f("bhck8431");
                let bhck8433 = f("bhck8433");
                let bhck8434 = f("bhck8434");
                let bhck8492 = f("bhck8492");
                let bhck8493 = f("bhck8493");
                let bhck8494 = f("bhck8494");
                let bhck8495 = f("bhck8495");
                let bhck8496 = f("bhck8496");
                let bhck8497 = f("bhck8497");
                let bhck8498 = f("bhck8498");
                let bhck8499 = f("bhck8499");
                let bhck8531 = f("bhck8531");
                let bhck8532 = f("bhck8532");
                let bhck8533 = f("bhck8533");
                let bhck8534 = f("bhck8534");
                let bhck8535 = f("bhck8535");
                let bhck8536 = f("bhck8536");
                let bhck8537 = f("bhck8537");
                let bhck8538 = f("bhck8538");
                let bhck8539 = f("bhck8539");
                let bhck8540 = f("bhck8540");
                let bhck8541 = f("bhck8541");
                let bhck8542 = f("bhck8542");
                let bhck8543 = f("bhck8543");
                let bhck8544 = f("bhck8544");
                let bhck8545 = f("bhck8545");
                let bhck8546 = f("bhck8546");
                let bhck8547 = f("bhck8547");
                let bhck8548 = f("bhck8548");
                let bhck8549 = f("bhck8549");
                let bhck8550 = f("bhck8550");
                let bhck8557 = f("bhck8557");
                let bhck8558 = f("bhck8558");
                let bhck8559 = f("bhck8559");
                let bhck8560 = f("bhck8560");
                let bhck8561 = f("bhck8561");
                let bhck8562 = f("bhck8562");
                let bhck8563 = f("bhck8563");
                let bhck8564 = f("bhck8564");
                let bhck8565 = f("bhck8565");
                let bhck8566 = f("bhck8566");
                let bhck8567 = f("bhck8567");
                let bhck8693 = f("bhck8693");
                let bhck8694 = f("bhck8694");
                let bhck8695 = f("bhck8695");
                let bhck8696 = f("bhck8696");
                let bhck8697 = f("bhck8697");
                let bhck8698 = f("bhck8698");
                let bhck8699 = f("bhck8699");
                let bhck8700 = f("bhck8700");
                let bhck8719 = f("bhck8719");
                let bhck8720 = f("bhck8720");
                let bhck8733 = f("bhck8733");
                let bhck8734 = f("bhck8734");
                let bhck8735 = f("bhck8735");
                let bhck8736 = f("bhck8736");
                let bhck8737 = f("bhck8737");
                let bhck8738 = f("bhck8738");
                let bhck8739 = f("bhck8739");
                let bhck8740 = f("bhck8740");
                let bhck8741 = f("bhck8741");
                let bhck8742 = f("bhck8742");
                let bhck8743 = f("bhck8743");
                let bhck8744 = f("bhck8744");
                let bhck8745 = f("bhck8745");
                let bhck8746 = f("bhck8746");
                let bhck8747 = f("bhck8747");
                let bhck8748 = f("bhck8748");
                let bhck8749 = f("bhck8749");
                let bhck8750 = f("bhck8750");
                let bhck8751 = f("bhck8751");
                let bhck8752 = f("bhck8752");
                let bhck8753 = f("bhck8753");
                let bhck8754 = f("bhck8754");
                let bhck8755 = f("bhck8755");
                let bhck8756 = f("bhck8756");
                let bhck8757 = f("bhck8757");
                let bhck8758 = f("bhck8758");
                let bhck8759 = f("bhck8759");
                let bhck8760 = f("bhck8760");
                let bhck8761 = f("bhck8761");
                let bhck8762 = f("bhck8762");
                let bhck8763 = f("bhck8763");
                let bhck8764 = f("bhck8764");
                let bhck8766 = f("bhck8766");
                let bhck8767 = f("bhck8767");
                let bhck8769 = f("bhck8769");
                let bhck8770 = f("bhck8770");
                let bhck8771 = f("bhck8771");
                let bhck8772 = f("bhck8772");
                let bhck8773 = f("bhck8773");
                let bhck8774 = f("bhck8774");
                let bhck8775 = f("bhck8775");
                let bhck8776 = f("bhck8776");
                let bhck8777 = f("bhck8777");
                let bhck8778 = f("bhck8778");
                let bhck8779 = f("bhck8779");
                let bhck8782 = f("bhck8782");
                let bhck8783 = f("bhck8783");
                let bhck8843 = f("bhck8843");
                let bhcka000 = f("bhcka000");
                let bhcka001 = f("bhcka001");
                let bhcka002 = f("bhcka002");
                let bhcka130 = f("bhcka130");
                let bhcka221 = f("bhcka221");
                let bhcka222 = f("bhcka222");
                let bhcka224 = f("bhcka224");
                let bhcka250 = f("bhcka250");
                let bhcka251 = f("bhcka251");
                let bhcka506 = f("bhcka506");
                let bhcka507 = f("bhcka507");
                let bhcka510 = f("bhcka510");
                let bhcka511 = f("bhcka511");
                let bhcka512 = f("bhcka512");
                let bhcka517 = f("bhcka517");
                let bhcka518 = f("bhcka518");
                let bhcka519 = f("bhcka519");
                let bhcka520 = f("bhcka520");
                let bhcka521 = f("bhcka521");
                let bhcka522 = f("bhcka522");
                let bhcka523 = f("bhcka523");
                let bhcka524 = f("bhcka524");
                let bhcka525 = f("bhcka525");
                let bhcka530 = f("bhcka530");
                let bhcka534 = f("bhcka534");
                let bhcka535 = f("bhcka535");
                let bhckb026 = f("bhckb026");
                let bhckb029 = f("bhckb029");
                let bhckb030 = f("bhckb030");
                let bhckb032 = f("bhckb032");
                let bhckb035 = f("bhckb035");
                let bhckb036 = f("bhckb036");
                let bhckb039 = f("bhckb039");
                let bhckb040 = f("bhckb040");
                let bhckb044 = f("bhckb044");
                let bhckb045 = f("bhckb045");
                let bhckb047 = f("bhckb047");
                let bhckb050 = f("bhckb050");
                let bhckb051 = f("bhckb051");
                let bhckb054 = f("bhckb054");
                let bhckb055 = f("bhckb055");
                let bhckb077 = f("bhckb077");
                let bhckb488 = f("bhckb488");
                let bhckb489 = f("bhckb489");
                let bhckb490 = f("bhckb490");
                let bhckb492 = f("bhckb492");
                let bhckb493 = f("bhckb493");
                let bhckb494 = f("bhckb494");
                let bhckb496 = f("bhckb496");
                let bhckb497 = f("bhckb497");
                let bhckb500 = f("bhckb500");
                let bhckb501 = f("bhckb501");
                let bhckb502 = f("bhckb502");
                let bhckb508 = f("bhckb508");
                let bhckb511 = f("bhckb511");
                let bhckb512 = f("bhckb512");
                let bhckb514 = f("bhckb514");
                let bhckb516 = f("bhckb516");
                let bhckb522 = f("bhckb522");
                let bhckb528 = f("bhckb528");
                let bhckb529 = f("bhckb529");
                let bhckb530 = f("bhckb530");
                let bhckb538 = f("bhckb538");
                let bhckb539 = f("bhckb539");
                let bhckb546 = f("bhckb546");
                let bhckb556 = f("bhckb556");
                let bhckb557 = f("bhckb557");
                let bhckb559 = f("bhckb559");
                let bhckb560 = f("bhckb560");
                let bhckb569 = f("bhckb569");
                let bhckb570 = f("bhckb570");
                let bhckb572 = f("bhckb572");
                let bhckb573 = f("bhckb573");
                let bhckb574 = f("bhckb574");
                let bhckb575 = f("bhckb575");
                let bhckb576 = f("bhckb576");
                let bhckb577 = f("bhckb577");
                let bhckb578 = f("bhckb578");
                let bhckb579 = f("bhckb579");
                let bhckb580 = f("bhckb580");
                let bhckb588 = f("bhckb588");
                let bhckb590 = f("bhckb590");
                let bhckb591 = f("bhckb591");
                let bhckb592 = f("bhckb592");
                let bhckb593 = f("bhckb593");
                let bhckb594 = f("bhckb594");
                let bhckb595 = f("bhckb595");
                let bhckb596 = f("bhckb596");
                let bhckb639 = f("bhckb639");
                let bhckb675 = f("bhckb675");
                let bhckb681 = f("bhckb681");
                let bhckb747 = f("bhckb747");
                let bhckb748 = f("bhckb748");
                let bhckb749 = f("bhckb749");
                let bhckb750 = f("bhckb750");
                let bhckb751 = f("bhckb751");
                let bhckb752 = f("bhckb752");
                let bhckb753 = f("bhckb753");
                let bhckb761 = f("bhckb761");
                let bhckb762 = f("bhckb762");
                let bhckb763 = f("bhckb763");
                let bhckb770 = f("bhckb770");
                let bhckb771 = f("bhckb771");
                let bhckb772 = f("bhckb772");
                let bhckb776 = f("bhckb776");
                let bhckb777 = f("bhckb777");
                let bhckb778 = f("bhckb778");
                let bhckb779 = f("bhckb779");
                let bhckb780 = f("bhckb780");
                let bhckb781 = f("bhckb781");
                let bhckb782 = f("bhckb782");
                let bhckb790 = f("bhckb790");
                let bhckb791 = f("bhckb791");
                let bhckb792 = f("bhckb792");
                let bhckb793 = f("bhckb793");
                let bhckb794 = f("bhckb794");
                let bhckb795 = f("bhckb795");
                let bhckb796 = f("bhckb796");
                let bhckb797 = f("bhckb797");
                let bhckb798 = f("bhckb798");
                let bhckb799 = f("bhckb799");
                let bhckb800 = f("bhckb800");
                let bhckb801 = f("bhckb801");
                let bhckb802 = f("bhckb802");
                let bhckb803 = f("bhckb803");
                let bhckb806 = f("bhckb806");
                let bhckb807 = f("bhckb807");
                let bhckb837 = f("bhckb837");
                let bhckb838 = f("bhckb838");
                let bhckb839 = f("bhckb839");
                let bhckb840 = f("bhckb840");
                let bhckb841 = f("bhckb841");
                let bhckb842 = f("bhckb842");
                let bhckb843 = f("bhckb843");
                let bhckb844 = f("bhckb844");
                let bhckb845 = f("bhckb845");
                let bhckb846 = f("bhckb846");
                let bhckb847 = f("bhckb847");
                let bhckb848 = f("bhckb848");
                let bhckb849 = f("bhckb849");
                let bhckb850 = f("bhckb850");
                let bhckb851 = f("bhckb851");
                let bhckb852 = f("bhckb852");
                let bhckb853 = f("bhckb853");
                let bhckb854 = f("bhckb854");
                let bhckb855 = f("bhckb855");
                let bhckb856 = f("bhckb856");
                let bhckb857 = f("bhckb857");
                let bhckb858 = f("bhckb858");
                let bhckb859 = f("bhckb859");
                let bhckb860 = f("bhckb860");
                let bhckb861 = f("bhckb861");
                let bhckb983 = f("bhckb983");
                let bhckb984 = f("bhckb984");
                let bhckb985 = f("bhckb985");
                let bhckb986 = b("bhckb986");
                let bhckb988 = f("bhckb988");
                let bhckb990 = f("bhckb990");
                let bhckb991 = f("bhckb991");
                let bhckb992 = f("bhckb992");
                let bhckb994 = f("bhckb994");
                let bhckb996 = f("bhckb996");
                let bhckb998 = f("bhckb998");
                let bhckc009 = f("bhckc009");
                let bhckc013 = f("bhckc013");
                let bhckc014 = f("bhckc014");
                let bhckc016 = f("bhckc016");
                let bhckc017 = f("bhckc017");
                let bhckc050 = b("bhckc050");
                let bhckc079 = f("bhckc079");
                let bhckc159 = f("bhckc159");
                let bhckc160 = f("bhckc160");
                let bhckc161 = f("bhckc161");
                let bhckc216 = f("bhckc216");
                let bhckc219 = f("bhckc219");
                let bhckc220 = f("bhckc220");
                let bhckc221 = f("bhckc221");
                let bhckc222 = f("bhckc222");
                let bhckc225 = f("bhckc225");
                let bhckc226 = f("bhckc226");
                let bhckc229 = f("bhckc229");
                let bhckc230 = f("bhckc230");
                let bhckc231 = f("bhckc231");
                let bhckc232 = f("bhckc232");
                let bhckc233 = f("bhckc233");
                let bhckc234 = f("bhckc234");
                let bhckc235 = f("bhckc235");
                let bhckc236 = f("bhckc236");
                let bhckc237 = f("bhckc237");
                let bhckc238 = f("bhckc238");
                let bhckc239 = f("bhckc239");
                let bhckc240 = f("bhckc240");
                let bhckc241 = f("bhckc241");
                let bhckc243 = f("bhckc243");
                let bhckc246 = f("bhckc246");
                let bhckc250 = f("bhckc250");
                let bhckc251 = f("bhckc251");
                let bhckc252 = f("bhckc252");
                let bhckc253 = f("bhckc253");
                let bhckc386 = f("bhckc386");
                let bhckc387 = f("bhckc387");
                let bhckc390 = f("bhckc390");
                let bhckc410 = f("bhckc410");
                let bhckc411 = f("bhckc411");
                let bhckc435 = f("bhckc435");
                let bhckc447 = f("bhckc447");
                let bhckc498 = f("bhckc498");
                let bhckc700 = f("bhckc700");
                let bhckc701 = f("bhckc701");
                let bhckc781 = f("bhckc781");
                let bhckc880 = f("bhckc880");
                let bhckc884 = f("bhckc884");
                let bhckc886 = f("bhckc886");
                let bhckc887 = f("bhckc887");
                let bhckc888 = f("bhckc888");
                let bhckc889 = f("bhckc889");
                let bhckc890 = f("bhckc890");
                let bhckc891 = f("bhckc891");
                let bhckc892 = f("bhckc892");
                let bhckc893 = f("bhckc893");
                let bhckc894 = f("bhckc894");
                let bhckc895 = f("bhckc895");
                let bhckc896 = f("bhckc896");
                let bhckc897 = f("bhckc897");
                let bhckc898 = f("bhckc898");
                let bhckc968 = f("bhckc968");
                let bhckc969 = f("bhckc969");
                let bhckc970 = f("bhckc970");
                let bhckc971 = f("bhckc971");
                let bhckc972 = f("bhckc972");
                let bhckc973 = f("bhckc973");
                let bhckc974 = f("bhckc974");
                let bhckc975 = f("bhckc975");
                let bhckc980 = f("bhckc980");
                let bhckc981 = f("bhckc981");
                let bhckc982 = f("bhckc982");
                let bhckc983 = f("bhckc983");
                let bhckc984 = f("bhckc984");
                let bhckc985 = f("bhckc985");
                let bhckc988 = f("bhckc988");
                let bhckc989 = f("bhckc989");
                let bhckd958 = f("bhckd958");
                let bhckd959 = f("bhckd959");
                let bhckd960 = f("bhckd960");
                let bhckd962 = f("bhckd962");
                let bhckd963 = f("bhckd963");
                let bhckd964 = f("bhckd964");
                let bhckd965 = f("bhckd965");
                let bhckd967 = f("bhckd967");
                let bhckd968 = f("bhckd968");
                let bhckd969 = f("bhckd969");
                let bhckd970 = f("bhckd970");
                let bhckd971 = f("bhckd971");
                let bhckd972 = f("bhckd972");
                let bhckd973 = f("bhckd973");
                let bhckd974 = f("bhckd974");
                let bhckd982 = f("bhckd982");
                let bhckd983 = f("bhckd983");
                let bhckd984 = f("bhckd984");
                let bhckd985 = f("bhckd985");
                let bhckd991 = f("bhckd991");
                let bhckd992 = f("bhckd992");
                let bhckd993 = f("bhckd993");
                let bhckd994 = f("bhckd994");
                let bhckd995 = f("bhckd995");
                let bhckd996 = f("bhckd996");
                let bhckf031 = f("bhckf031");
                let bhckf070 = f("bhckf070");
                let bhckf071 = f("bhckf071");
                let bhckf072 = f("bhckf072");
                let bhckf073 = f("bhckf073");
                let bhckf158 = f("bhckf158");
                let bhckf159 = f("bhckf159");
                let bhckf160 = f("bhckf160");
                let bhckf161 = f("bhckf161");
                let bhckf162 = f("bhckf162");
                let bhckf163 = f("bhckf163");
                let bhckf164 = f("bhckf164");
                let bhckf165 = f("bhckf165");
                let bhckf166 = f("bhckf166");
                let bhckf167 = f("bhckf167");
                let bhckf168 = f("bhckf168");
                let bhckf169 = f("bhckf169");
                let bhckf170 = f("bhckf170");
                let bhckf171 = f("bhckf171");
                let bhckf172 = f("bhckf172");
                let bhckf173 = f("bhckf173");
                let bhckf174 = f("bhckf174");
                let bhckf175 = f("bhckf175");
                let bhckf176 = f("bhckf176");
                let bhckf177 = f("bhckf177");
                let bhckf178 = f("bhckf178");
                let bhckf179 = f("bhckf179");
                let bhckf180 = f("bhckf180");
                let bhckf181 = f("bhckf181");
                let bhckf182 = f("bhckf182");
                let bhckf183 = f("bhckf183");
                let bhckf184 = f("bhckf184");
                let bhckf185 = f("bhckf185");
                let bhckf228 = f("bhckf228");
                let bhckf229 = f("bhckf229");
                let bhckf241 = f("bhckf241");
                let bhckf242 = f("bhckf242");
                let bhckf244 = f("bhckf244");
                let bhckf245 = f("bhckf245");
                let bhckf247 = f("bhckf247");
                let bhckf248 = f("bhckf248");
                let bhckf250 = f("bhckf250");
                let bhckf251 = f("bhckf251");
                let bhckf253 = f("bhckf253");
                let bhckf254 = f("bhckf254");
                let bhckf256 = f("bhckf256");
                let bhckf257 = f("bhckf257");
                let bhckf259 = f("bhckf259");
                let bhckf260 = f("bhckf260");
                let bhckf262 = f("bhckf262");
                let bhckf263 = f("bhckf263");
                let bhckf264 = f("bhckf264");
                let bhckf465 = f("bhckf465");
                let bhckf551 = f("bhckf551");
                let bhckf552 = f("bhckf552");
                let bhckf553 = f("bhckf553");
                let bhckf554 = f("bhckf554");
                let bhckf555 = f("bhckf555");
                let bhckf556 = f("bhckf556");
                let bhckf557 = f("bhckf557");
                let bhckf558 = f("bhckf558");
                let bhckf585 = f("bhckf585");
                let bhckf586 = f("bhckf586");
                let bhckf587 = f("bhckf587");
                let bhckf588 = f("bhckf588");
                let bhckf589 = f("bhckf589");
                let bhckf608 = f("bhckf608");
                let bhckf639 = f("bhckf639");
                let bhckf640 = f("bhckf640");
                let bhckf655 = f("bhckf655");
                let bhckf658 = f("bhckf658");
                let bhckf661 = f("bhckf661");
                let bhckf662 = f("bhckf662");
                let bhckf663 = f("bhckf663");
                let bhckf664 = f("bhckf664");
                let bhckf665 = f("bhckf665");
                let bhckf666 = f("bhckf666");
                let bhckf682 = f("bhckf682");
                let bhckf683 = f("bhckf683");
                let bhckf684 = f("bhckf684");
                let bhckf685 = f("bhckf685");
                let bhckf686 = f("bhckf686");
                let bhckf687 = f("bhckf687");
                let bhckf688 = f("bhckf688");
                let bhckf689 = f("bhckf689");
                let bhckf690 = f("bhckf690");
                let bhckf691 = f("bhckf691");
                let bhckf692 = f("bhckf692");
                let bhckf693 = f("bhckf693");
                let bhckf694 = f("bhckf694");
                let bhckf695 = f("bhckf695");
                let bhckf696 = f("bhckf696");
                let bhckf697 = f("bhckf697");
                let bhckf821 = f("bhckf821");
                let bhckf841 = b("bhckf841");
                let bhckft28 = f("bhckft28");
                let bhckft29 = f("bhckft29");
                let bhckft30 = f("bhckft30");
                let bhckft31 = f("bhckft31");
                let bhckft32 = f("bhckft32");
                let bhckft41 = f("bhckft41");
                let bhckft42 = b("bhckft42");
                let bhckft43 = b("bhckft43");
                let bhckft44 = b("bhckft44");
                let bhckg091 = f("bhckg091");
                let bhckg092 = f("bhckg092");
                let bhckg093 = f("bhckg093");
                let bhckg094 = f("bhckg094");
                let bhckg095 = f("bhckg095");
                let bhckg096 = f("bhckg096");
                let bhckg097 = f("bhckg097");
                let bhckg098 = f("bhckg098");
                let bhckg099 = f("bhckg099");
                let bhckg100 = f("bhckg100");
                let bhckg101 = f("bhckg101");
                let bhckg102 = f("bhckg102");
                let bhckg103 = f("bhckg103");
                let bhckg104 = f("bhckg104");
                let bhckg209 = f("bhckg209");
                let bhckg210 = f("bhckg210");
                let bhckg211 = f("bhckg211");
                let bhckg212 = f("bhckg212");
                let bhckg213 = f("bhckg213");
                let bhckg218 = f("bhckg218");
                let bhckg221 = f("bhckg221");
                let bhckg234 = f("bhckg234");
                let bhckg235 = f("bhckg235");
                let bhckg300 = f("bhckg300");
                let bhckg301 = f("bhckg301");
                let bhckg302 = f("bhckg302");
                let bhckg303 = f("bhckg303");
                let bhckg304 = f("bhckg304");
                let bhckg305 = f("bhckg305");
                let bhckg306 = f("bhckg306");
                let bhckg307 = f("bhckg307");
                let bhckg308 = f("bhckg308");
                let bhckg309 = f("bhckg309");
                let bhckg310 = f("bhckg310");
                let bhckg311 = f("bhckg311");
                let bhckg312 = f("bhckg312");
                let bhckg313 = f("bhckg313");
                let bhckg314 = f("bhckg314");
                let bhckg315 = f("bhckg315");
                let bhckg316 = f("bhckg316");
                let bhckg317 = f("bhckg317");
                let bhckg318 = f("bhckg318");
                let bhckg319 = f("bhckg319");
                let bhckg320 = f("bhckg320");
                let bhckg321 = f("bhckg321");
                let bhckg322 = f("bhckg322");
                let bhckg323 = f("bhckg323");
                let bhckg324 = f("bhckg324");
                let bhckg325 = f("bhckg325");
                let bhckg326 = f("bhckg326");
                let bhckg327 = f("bhckg327");
                let bhckg328 = f("bhckg328");
                let bhckg329 = f("bhckg329");
                let bhckg330 = f("bhckg330");
                let bhckg331 = f("bhckg331");
                let bhckg336 = f("bhckg336");
                let bhckg337 = f("bhckg337");
                let bhckg338 = f("bhckg338");
                let bhckg339 = f("bhckg339");
                let bhckg340 = f("bhckg340");
                let bhckg341 = f("bhckg341");
                let bhckg342 = f("bhckg342");
                let bhckg343 = f("bhckg343");
                let bhckg344 = f("bhckg344");
                let bhckg345 = f("bhckg345");
                let bhckg346 = f("bhckg346");
                let bhckg347 = f("bhckg347");
                let bhckg391 = f("bhckg391");
                let bhckg392 = f("bhckg392");
                let bhckg395 = f("bhckg395");
                let bhckg396 = f("bhckg396");
                let bhckg401 = f("bhckg401");
                let bhckg402 = f("bhckg402");
                let bhckg403 = f("bhckg403");
                let bhckg404 = f("bhckg404");
                let bhckg405 = f("bhckg405");
                let bhckg406 = f("bhckg406");
                let bhckg407 = f("bhckg407");
                let bhckg408 = f("bhckg408");
                let bhckg409 = f("bhckg409");
                let bhckg410 = f("bhckg410");
                let bhckg411 = f("bhckg411");
                let bhckg412 = f("bhckg412");
                let bhckg413 = f("bhckg413");
                let bhckg414 = f("bhckg414");
                let bhckg415 = f("bhckg415");
                let bhckg416 = f("bhckg416");
                let bhckg417 = f("bhckg417");
                let bhckg474 = f("bhckg474");
                let bhckg475 = f("bhckg475");
                let bhckg476 = f("bhckg476");
                let bhckg477 = f("bhckg477");
                let bhckg478 = f("bhckg478");
                let bhckg479 = f("bhckg479");
                let bhckg480 = f("bhckg480");
                let bhckg481 = f("bhckg481");
                let bhckg482 = f("bhckg482");
                let bhckg483 = f("bhckg483");
                let bhckg484 = f("bhckg484");
                let bhckg485 = f("bhckg485");
                let bhckg486 = f("bhckg486");
                let bhckg487 = f("bhckg487");
                let bhckg488 = f("bhckg488");
                let bhckg489 = f("bhckg489");
                let bhckg490 = f("bhckg490");
                let bhckg491 = f("bhckg491");
                let bhckg492 = f("bhckg492");
                let bhckg507 = f("bhckg507");
                let bhckg508 = f("bhckg508");
                let bhckg509 = f("bhckg509");
                let bhckg510 = f("bhckg510");
                let bhckg511 = f("bhckg511");
                let bhckg521 = f("bhckg521");
                let bhckg522 = f("bhckg522");
                let bhckg523 = f("bhckg523");
                let bhckg524 = f("bhckg524");
                let bhckg525 = f("bhckg525");
                let bhckg536 = f("bhckg536");
                let bhckg537 = f("bhckg537");
                let bhckg538 = f("bhckg538");
                let bhckg539 = f("bhckg539");
                let bhckg540 = f("bhckg540");
                let bhckg541 = f("bhckg541");
                let bhckg542 = f("bhckg542");
                let bhckg543 = f("bhckg543");
                let bhckg544 = f("bhckg544");
                let bhckg545 = f("bhckg545");
                let bhckg546 = f("bhckg546");
                let bhckg547 = f("bhckg547");
                let bhckg548 = f("bhckg548");
                let bhckg549 = f("bhckg549");
                let bhckg550 = f("bhckg550");
                let bhckg561 = f("bhckg561");
                let bhckg562 = f("bhckg562");
                let bhckg563 = f("bhckg563");
                let bhckg564 = f("bhckg564");
                let bhckg565 = f("bhckg565");
                let bhckg566 = f("bhckg566");
                let bhckg567 = f("bhckg567");
                let bhckg568 = f("bhckg568");
                let bhckg569 = f("bhckg569");
                let bhckg570 = f("bhckg570");
                let bhckg571 = f("bhckg571");
                let bhckg572 = f("bhckg572");
                let bhckg573 = f("bhckg573");
                let bhckg574 = f("bhckg574");
                let bhckg575 = f("bhckg575");
                let bhckg586 = f("bhckg586");
                let bhckg587 = f("bhckg587");
                let bhckg588 = f("bhckg588");
                let bhckg589 = f("bhckg589");
                let bhckg590 = f("bhckg590");
                let bhckg597 = f("bhckg597");
                let bhckg598 = f("bhckg598");
                let bhckg599 = f("bhckg599");
                let bhckg600 = f("bhckg600");
                let bhckg601 = f("bhckg601");
                let bhckg602 = f("bhckg602");
                let bhckg606 = f("bhckg606");
                let bhckg607 = f("bhckg607");
                let bhckg608 = f("bhckg608");
                let bhckg609 = f("bhckg609");
                let bhckg610 = f("bhckg610");
                let bhckg611 = f("bhckg611");
                let bhckg618 = f("bhckg618");
                let bhckg619 = f("bhckg619");
                let bhckg620 = f("bhckg620");
                let bhckg621 = f("bhckg621");
                let bhckg622 = f("bhckg622");
                let bhckg623 = f("bhckg623");
                let bhckg642 = f("bhckg642");
                let bhckg804 = f("bhckg804");
                let bhckg805 = f("bhckg805");
                let bhckg806 = f("bhckg806");
                let bhckg807 = f("bhckg807");
                let bhckg808 = f("bhckg808");
                let bhckg809 = f("bhckg809");
                let bhckg894 = f("bhckg894");
                let bhckg914 = f("bhckg914");
                let bhckh172 = f("bhckh172");
                let bhckh173 = f("bhckh173");
                let bhckh174 = f("bhckh174");
                let bhckh175 = f("bhckh175");
                let bhckh176 = f("bhckh176");
                let bhckh177 = f("bhckh177");
                let bhckh178 = f("bhckh178");
                let bhckh179 = f("bhckh179");
                let bhckh180 = f("bhckh180");
                let bhckh181 = f("bhckh181");
                let bhckh182 = f("bhckh182");
                let bhckh185 = f("bhckh185");
                let bhckh186 = f("bhckh186");
                let bhckh187 = f("bhckh187");
                let bhckh188 = f("bhckh188");
                let bhckh193 = f("bhckh193");
                let bhckh194 = f("bhckh194");
                let bhckh195 = f("bhckh195");
                let bhckh196 = f("bhckh196");
                let bhckh197 = f("bhckh197");
                let bhckh198 = f("bhckh198");
                let bhckh199 = f("bhckh199");
                let bhckh200 = f("bhckh200");
                let bhckh270 = f("bhckh270");
                let bhckh271 = f("bhckh271");
                let bhckh272 = f("bhckh272");
                let bhckh273 = f("bhckh273");
                let bhckh274 = f("bhckh274");
                let bhckh275 = f("bhckh275");
                let bhckh276 = f("bhckh276");
                let bhckh277 = f("bhckh277");
                let bhckh278 = f("bhckh278");
                let bhckh279 = f("bhckh279");
                let bhckh280 = f("bhckh280");
                let bhckh281 = f("bhckh281");
                let bhckh282 = f("bhckh282");
                let bhckh283 = f("bhckh283");
                let bhckh284 = f("bhckh284");
                let bhckh285 = f("bhckh285");
                let bhckh286 = f("bhckh286");
                let bhckh287 = f("bhckh287");
                let bhckh288 = f("bhckh288");
                let bhckh293 = f("bhckh293");
                let bhckh294 = f("bhckh294");
                let bhckh295 = f("bhckh295");
                let bhckh296 = f("bhckh296");
                let bhckh297 = f("bhckh297");
                let bhckh298 = f("bhckh298");
                let bhckh299 = f("bhckh299");
                let bhckhj78 = f("bhckhj78");
                let bhckhj79 = f("bhckhj79");
                let bhckhj80 = f("bhckhj80");
                let bhckhj81 = f("bhckhj81");
                let bhckhj82 = f("bhckhj82");
                let bhckhj83 = f("bhckhj83");
                let bhckhj84 = f("bhckhj84");
                let bhckhj85 = f("bhckhj85");
                let bhckhj88 = f("bhckhj88");
                let bhckhj89 = f("bhckhj89");
                let bhckhj92 = f("bhckhj92");
                let bhckhj93 = f("bhckhj93");
                let bhckhj94 = f("bhckhj94");
                let bhckhj95 = f("bhckhj95");
                let bhckhk03 = f("bhckhk03");
                let bhckhk04 = f("bhckhk04");
                let bhckht58 = f("bhckht58");
                let bhckht59 = f("bhckht59");
                let bhckht60 = f("bhckht60");
                let bhckht61 = f("bhckht61");
                let bhckht62 = f("bhckht62");
                let bhckht63 = f("bhckht63");
                let bhckht64 = f("bhckht64");
                let bhckht65 = f("bhckht65");
                let bhckht69 = f("bhckht69");
                let bhckht80 = f("bhckht80");
                let bhckht83 = f("bhckht83");
                let bhckht84 = f("bhckht84");
                let bhckht85 = f("bhckht85");
                let bhckht87 = f("bhckht87");
                let bhckht88 = f("bhckht88");
                let bhckht89 = f("bhckht89");
                let bhckht91 = f("bhckht91");
                let bhckht92 = f("bhckht92");
                let bhckht93 = f("bhckht93");
                let bhckhu09 = f("bhckhu09");
                let bhckhu10 = f("bhckhu10");
                let bhckhu11 = f("bhckhu11");
                let bhckhu12 = f("bhckhu12");
                let bhckhu13 = f("bhckhu13");
                let bhckhu14 = f("bhckhu14");
                let bhckhu15 = f("bhckhu15");
                let bhckhu20 = f("bhckhu20");
                let bhckhu21 = f("bhckhu21");
                let bhckhu22 = f("bhckhu22");
                let bhckhu23 = f("bhckhu23");
                let bhckj320 = f("bhckj320");
                let bhckj447 = f("bhckj447");
                let bhckj451 = f("bhckj451");
                let bhckj452 = f("bhckj452");
                let bhckj453 = f("bhckj453");
                let bhckj454 = f("bhckj454");
                let bhckj455 = f("bhckj455");
                let bhckj456 = f("bhckj456");
                let bhckj461 = f("bhckj461");
                let bhckj462 = f("bhckj462");
                let bhckj463 = f("bhckj463");
                let bhckj536 = f("bhckj536");
                let bhckj537 = f("bhckj537");
                let bhckj981 = f("bhckj981");
                let bhckj982 = f("bhckj982");
                let bhckj983 = f("bhckj983");
                let bhckj984 = f("bhckj984");
                let bhckj985 = f("bhckj985");
                let bhckj986 = f("bhckj986");
                let bhckj987 = f("bhckj987");
                let bhckj988 = f("bhckj988");
                let bhckj989 = f("bhckj989");
                let bhckj990 = f("bhckj990");
                let bhckj991 = f("bhckj991");
                let bhckj992 = f("bhckj992");
                let bhckj993 = f("bhckj993");
                let bhckj994 = f("bhckj994");
                let bhckj995 = f("bhckj995");
                let bhckj996 = f("bhckj996");
                let bhckj997 = f("bhckj997");
                let bhckj998 = f("bhckj998");
                let bhckj999 = f("bhckj999");
                let bhckja21 = f("bhckja21");
                let bhckja22 = f("bhckja22");
                let bhckjf76 = f("bhckjf76");
                let bhckjf84 = f("bhckjf84");
                let bhckjf85 = f("bhckjf85");
                let bhckjf86 = f("bhckjf86");
                let bhckjf87 = f("bhckjf87");
                let bhckjf88 = f("bhckjf88");
                let bhckjf89 = f("bhckjf89");
                let bhckjf90 = f("bhckjf90");
                let bhckjf91 = f("bhckjf91");
                let bhckjf92 = f("bhckjf92");
                let bhckjf93 = f("bhckjf93");
                let bhckjh88 = f("bhckjh88");
                let bhckjh91 = f("bhckjh91");
                let bhckjh92 = f("bhckjh92");
                let bhckjh93 = f("bhckjh93");
                let bhckjh94 = f("bhckjh94");
                let bhckjh97 = f("bhckjh97");
                let bhckjh98 = f("bhckjh98");
                let bhckjh99 = f("bhckjh99");
                let bhckjj00 = f("bhckjj00");
                let bhckjj01 = f("bhckjj01");
                let bhckjj03 = f("bhckjj03");
                let bhckjj04 = f("bhckjj04");
                let bhckjj05 = f("bhckjj05");
                let bhckjj06 = f("bhckjj06");
                let bhckjj07 = f("bhckjj07");
                let bhckjj08 = f("bhckjj08");
                let bhckjj09 = f("bhckjj09");
                let bhckjj11 = f("bhckjj11");
                let bhckjj12 = f("bhckjj12");
                let bhckjj13 = f("bhckjj13");
                let bhckjj14 = f("bhckjj14");
                let bhckjj15 = f("bhckjj15");
                let bhckjj16 = f("bhckjj16");
                let bhckjj17 = f("bhckjj17");
                let bhckjj18 = f("bhckjj18");
                let bhckjj19 = f("bhckjj19");
                let bhckjj20 = f("bhckjj20");
                let bhckjj21 = f("bhckjj21");
                let bhckjj23 = f("bhckjj23");
                let bhckjj24 = f("bhckjj24");
                let bhckjj25 = f("bhckjj25");
                let bhckjj26 = f("bhckjj26");
                let bhckjj27 = f("bhckjj27");
                let bhckjj28 = f("bhckjj28");
                let bhckjj30 = f("bhckjj30");
                let bhckjj31 = f("bhckjj31");
                let bhckjj32 = f("bhckjj32");
                let bhckjj34 = f("bhckjj34");
                let bhckk001 = f("bhckk001");
                let bhckk002 = f("bhckk002");
                let bhckk003 = f("bhckk003");
                let bhckk004 = f("bhckk004");
                let bhckk005 = f("bhckk005");
                let bhckk006 = f("bhckk006");
                let bhckk007 = f("bhckk007");
                let bhckk008 = f("bhckk008");
                let bhckk009 = f("bhckk009");
                let bhckk010 = f("bhckk010");
                let bhckk011 = f("bhckk011");
                let bhckk012 = f("bhckk012");
                let bhckk013 = f("bhckk013");
                let bhckk014 = f("bhckk014");
                let bhckk015 = f("bhckk015");
                let bhckk016 = f("bhckk016");
                let bhckk017 = f("bhckk017");
                let bhckk018 = f("bhckk018");
                let bhckk019 = f("bhckk019");
                let bhckk020 = f("bhckk020");
                let bhckk021 = f("bhckk021");
                let bhckk022 = f("bhckk022");
                let bhckk023 = f("bhckk023");
                let bhckk024 = f("bhckk024");
                let bhckk025 = f("bhckk025");
                let bhckk026 = f("bhckk026");
                let bhckk027 = f("bhckk027");
                let bhckk028 = f("bhckk028");
                let bhckk029 = f("bhckk029");
                let bhckk030 = f("bhckk030");
                let bhckk031 = f("bhckk031");
                let bhckk032 = f("bhckk032");
                let bhckk033 = f("bhckk033");
                let bhckk034 = f("bhckk034");
                let bhckk035 = f("bhckk035");
                let bhckk036 = f("bhckk036");
                let bhckk037 = f("bhckk037");
                let bhckk038 = f("bhckk038");
                let bhckk039 = f("bhckk039");
                let bhckk040 = f("bhckk040");
                let bhckk041 = f("bhckk041");
                let bhckk072 = f("bhckk072");
                let bhckk073 = f("bhckk073");
                let bhckk074 = f("bhckk074");
                let bhckk075 = f("bhckk075");
                let bhckk076 = f("bhckk076");
                let bhckk077 = f("bhckk077");
                let bhckk078 = f("bhckk078");
                let bhckk079 = f("bhckk079");
                let bhckk080 = f("bhckk080");
                let bhckk081 = f("bhckk081");
                let bhckk082 = f("bhckk082");
                let bhckk083 = f("bhckk083");
                let bhckk084 = f("bhckk084");
                let bhckk085 = f("bhckk085");
                let bhckk086 = f("bhckk086");
                let bhckk087 = f("bhckk087");
                let bhckk088 = f("bhckk088");
                let bhckk089 = f("bhckk089");
                let bhckk090 = f("bhckk090");
                let bhckk091 = f("bhckk091");
                let bhckk092 = f("bhckk092");
                let bhckk093 = f("bhckk093");
                let bhckk094 = f("bhckk094");
                let bhckk095 = f("bhckk095");
                let bhckk096 = f("bhckk096");
                let bhckk097 = f("bhckk097");
                let bhckk098 = f("bhckk098");
                let bhckk099 = f("bhckk099");
                let bhckk100 = f("bhckk100");
                let bhckk101 = f("bhckk101");
                let bhckk120 = f("bhckk120");
                let bhckk121 = f("bhckk121");
                let bhckk122 = f("bhckk122");
                let bhckk123 = f("bhckk123");
                let bhckk124 = f("bhckk124");
                let bhckk125 = f("bhckk125");
                let bhckk126 = f("bhckk126");
                let bhckk127 = f("bhckk127");
                let bhckk128 = f("bhckk128");
                let bhckk129 = f("bhckk129");
                let bhckk134 = f("bhckk134");
                let bhckk135 = f("bhckk135");
                let bhckk136 = f("bhckk136");
                let bhckk137 = f("bhckk137");
                let bhckk138 = f("bhckk138");
                let bhckk139 = f("bhckk139");
                let bhckk140 = f("bhckk140");
                let bhckk142 = f("bhckk142");
                let bhckk143 = f("bhckk143");
                let bhckk144 = f("bhckk144");
                let bhckk145 = f("bhckk145");
                let bhckk146 = f("bhckk146");
                let bhckk147 = f("bhckk147");
                let bhckk148 = f("bhckk148");
                let bhckk149 = f("bhckk149");
                let bhckk150 = f("bhckk150");
                let bhckk151 = f("bhckk151");
                let bhckk152 = f("bhckk152");
                let bhckk153 = f("bhckk153");
                let bhckk154 = f("bhckk154");
                let bhckk155 = f("bhckk155");
                let bhckk156 = f("bhckk156");
                let bhckk157 = f("bhckk157");
                let bhckk163 = f("bhckk163");
                let bhckk164 = f("bhckk164");
                let bhckk165 = f("bhckk165");
                let bhckk167 = f("bhckk167");
                let bhckk168 = f("bhckk168");
                let bhckk178 = f("bhckk178");
                let bhckk179 = f("bhckk179");
                let bhckk180 = f("bhckk180");
                let bhckk181 = f("bhckk181");
                let bhckk182 = f("bhckk182");
                let bhckk183 = f("bhckk183");
                let bhckk184 = f("bhckk184");
                let bhckk185 = f("bhckk185");
                let bhckk186 = f("bhckk186");
                let bhckk192 = f("bhckk192");
                let bhckk193 = f("bhckk193");
                let bhckk194 = f("bhckk194");
                let bhckk196 = f("bhckk196");
                let bhckk201 = f("bhckk201");
                let bhckk202 = f("bhckk202");
                let bhckk203 = f("bhckk203");
                let bhckk204 = f("bhckk204");
                let bhckk205 = f("bhckk205");
                let bhckk207 = f("bhckk207");
                let bhckk208 = f("bhckk208");
                let bhckk212 = f("bhckk212");
                let bhckk213 = f("bhckk213");
                let bhckk214 = f("bhckk214");
                let bhckk215 = f("bhckk215");
                let bhckk216 = f("bhckk216");
                let bhckk217 = f("bhckk217");
                let bhckk218 = f("bhckk218");
                let bhckk267 = f("bhckk267");
                let bhckk269 = f("bhckk269");
                let bhckk270 = f("bhckk270");
                let bhckk271 = f("bhckk271");
                let bhckk272 = f("bhckk272");
                let bhckk273 = f("bhckk273");
                let bhckk274 = f("bhckk274");
                let bhckk275 = f("bhckk275");
                let bhckk276 = f("bhckk276");
                let bhckk277 = f("bhckk277");
                let bhckk278 = f("bhckk278");
                let bhckk279 = f("bhckk279");
                let bhckk280 = f("bhckk280");
                let bhckk281 = f("bhckk281");
                let bhckk282 = f("bhckk282");
                let bhckk283 = f("bhckk283");
                let bhckk284 = f("bhckk284");
                let bhckk285 = f("bhckk285");
                let bhckk286 = f("bhckk286");
                let bhckk287 = f("bhckk287");
                let bhckk288 = f("bhckk288");
                let bhckkx46 = f("bhckkx46");
                let bhckkx47 = f("bhckkx47");
                let bhckkx50 = f("bhckkx50");
                let bhckkx51 = f("bhckkx51");
                let bhckkx52 = f("bhckkx52");
                let bhckkx53 = f("bhckkx53");
                let bhckkx54 = f("bhckkx54");
                let bhckkx55 = f("bhckkx55");
                let bhckkx57 = f("bhckkx57");
                let bhckkx58 = f("bhckkx58");
                let bhckkx60 = f("bhckkx60");
                let bhckkx61 = f("bhckkx61");
                let bhckkx62 = f("bhckkx62");
                let bhckkx63 = f("bhckkx63");
                let bhckkx64 = f("bhckkx64");
                let bhckkx65 = f("bhckkx65");
                let bhckky38 = b("bhckky38");
                let bhcklg24 = b("bhcklg24");
                let bhcklg26 = f("bhcklg26");
                let bhckm727 = f("bhckm727");
                let bhckm728 = f("bhckm728");
                let bhckm729 = f("bhckm729");
                let bhckm730 = f("bhckm730");
                let bhckm731 = f("bhckm731");
                let bhckm732 = f("bhckm732");
                let bhckm733 = f("bhckm733");
                let bhckm734 = f("bhckm734");
                let bhckm735 = f("bhckm735");
                let bhckm736 = f("bhckm736");
                let bhckm737 = f("bhckm737");
                let bhckm738 = f("bhckm738");
                let bhckm739 = f("bhckm739");
                let bhckm740 = f("bhckm740");
                let bhckm741 = f("bhckm741");
                let bhckm742 = f("bhckm742");
                let bhckm743 = f("bhckm743");
                let bhckm744 = f("bhckm744");
                let bhckm962 = f("bhckm962");
                let bhckmg94 = f("bhckmg94");
                let bhcks396 = f("bhcks396");
                let bhcks397 = f("bhcks397");
                let bhcks398 = f("bhcks398");
                let bhcks399 = f("bhcks399");
                let bhcks400 = f("bhcks400");
                let bhcks402 = f("bhcks402");
                let bhcks403 = f("bhcks403");
                let bhcks405 = f("bhcks405");
                let bhcks406 = f("bhcks406");
                let bhcks410 = f("bhcks410");
                let bhcks411 = f("bhcks411");
                let bhcks414 = f("bhcks414");
                let bhcks415 = f("bhcks415");
                let bhcks416 = f("bhcks416");
                let bhcks417 = f("bhcks417");
                let bhcks420 = f("bhcks420");
                let bhcks421 = f("bhcks421");
                let bhcks424 = f("bhcks424");
                let bhcks425 = f("bhcks425");
                let bhcks426 = f("bhcks426");
                let bhcks427 = f("bhcks427");
                let bhcks428 = f("bhcks428");
                let bhcks429 = f("bhcks429");
                let bhcks432 = f("bhcks432");
                let bhcks433 = f("bhcks433");
                let bhcks434 = f("bhcks434");
                let bhcks435 = f("bhcks435");
                let bhcks436 = f("bhcks436");
                let bhcks437 = f("bhcks437");
                let bhcks440 = f("bhcks440");
                let bhcks441 = f("bhcks441");
                let bhcks442 = f("bhcks442");
                let bhcks443 = f("bhcks443");
                let bhcks446 = f("bhcks446");
                let bhcks447 = f("bhcks447");
                let bhcks450 = f("bhcks450");
                let bhcks451 = f("bhcks451");
                let bhcks452 = f("bhcks452");
                let bhcks453 = f("bhcks453");
                let bhcks454 = f("bhcks454");
                let bhcks455 = f("bhcks455");
                let bhcks458 = f("bhcks458");
                let bhcks459 = f("bhcks459");
                let bhcks460 = f("bhcks460");
                let bhcks461 = f("bhcks461");
                let bhcks462 = f("bhcks462");
                let bhcks463 = f("bhcks463");
                let bhcks469 = f("bhcks469");
                let bhcks470 = f("bhcks470");
                let bhcks471 = f("bhcks471");
                let bhcks476 = f("bhcks476");
                let bhcks477 = f("bhcks477");
                let bhcks478 = f("bhcks478");
                let bhcks479 = f("bhcks479");
                let bhcks481 = f("bhcks481");
                let bhcks482 = f("bhcks482");
                let bhcks483 = f("bhcks483");
                let bhcks484 = f("bhcks484");
                let bhcks486 = f("bhcks486");
                let bhcks487 = f("bhcks487");
                let bhcks488 = f("bhcks488");
                let bhcks489 = f("bhcks489");
                let bhcks491 = f("bhcks491");
                let bhcks492 = f("bhcks492");
                let bhcks493 = f("bhcks493");
                let bhcks494 = f("bhcks494");
                let bhcks496 = f("bhcks496");
                let bhcks497 = f("bhcks497");
                let bhcks498 = f("bhcks498");
                let bhcks499 = f("bhcks499");
                let bhcks511 = f("bhcks511");
                let bhcks513 = f("bhcks513");
                let bhcks524 = f("bhcks524");
                let bhcks549 = f("bhcks549");
                let bhcks550 = f("bhcks550");
                let bhcks551 = f("bhcks551");
                let bhcks552 = f("bhcks552");
                let bhcks554 = f("bhcks554");
                let bhcks555 = f("bhcks555");
                let bhcks556 = f("bhcks556");
                let bhcks557 = f("bhcks557");
                let bhcks582 = f("bhcks582");
                let bhcks583 = f("bhcks583");
                let bhcks584 = f("bhcks584");
                let bhcks585 = f("bhcks585");
                let bhcks586 = f("bhcks586");
                let bhcks587 = f("bhcks587");
                let bhcks588 = f("bhcks588");
                let bhcks589 = f("bhcks589");
                let bhcks590 = f("bhcks590");
                let bhcks591 = f("bhcks591");
                let bhcks592 = f("bhcks592");
                let bhcks593 = f("bhcks593");
                let bhcks594 = f("bhcks594");
                let bhcks595 = f("bhcks595");
                let bhcks596 = f("bhcks596");
                let bhcks597 = f("bhcks597");
                let bhcks598 = f("bhcks598");
                let bhcks599 = f("bhcks599");
                let bhcks600 = f("bhcks600");
                let bhcks601 = f("bhcks601");
                let bhcks602 = f("bhcks602");
                let bhcks603 = f("bhcks603");
                let bhcks604 = f("bhcks604");
                let bhcks605 = f("bhcks605");
                let bhcks606 = f("bhcks606");
                let bhcks607 = f("bhcks607");
                let bhcks608 = f("bhcks608");
                let bhcks609 = f("bhcks609");
                let bhcks610 = f("bhcks610");
                let bhcks611 = f("bhcks611");
                let bhcks612 = f("bhcks612");
                let bhcks613 = f("bhcks613");
                let bhcks614 = f("bhcks614");
                let bhcks615 = f("bhcks615");
                let bhcks616 = f("bhcks616");
                let bhcks617 = f("bhcks617");
                let bhcks618 = f("bhcks618");
                let bhcks619 = f("bhcks619");
                let bhcks620 = f("bhcks620");
                let bhcks621 = f("bhcks621");
                let bhcks622 = f("bhcks622");
                let bhcks623 = f("bhcks623");
                let bhckt047 = f("bhckt047");
                let bhcky923 = f("bhcky923");
                let bhcky924 = f("bhcky924");
                let rssd9001 = f("rssd9001");
                let rssd9017 = s("rssd9017");
                let rssd9999 = f("rssd9999");
                let wrdsdownloaddate = d("wrdsdownloaddate");

                for row_i in 0..batch.num_rows() {
                    let mut vals: Vec<AnyValue<'static>> = Vec::with_capacity(1501);
                    vals.push(if bhck0010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0010.value(row_i))
                    });
                    vals.push(if bhck0081.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0081.value(row_i))
                    });
                    vals.push(if bhck0211.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0211.value(row_i))
                    });
                    vals.push(if bhck0213.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0213.value(row_i))
                    });
                    vals.push(if bhck0379.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0379.value(row_i))
                    });
                    vals.push(if bhck0395.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0395.value(row_i))
                    });
                    vals.push(if bhck0397.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0397.value(row_i))
                    });
                    vals.push(if bhck0426.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0426.value(row_i))
                    });
                    vals.push(if bhck0497.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0497.value(row_i))
                    });
                    vals.push(if bhck1226.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1226.value(row_i))
                    });
                    vals.push(if bhck1227.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1227.value(row_i))
                    });
                    vals.push(if bhck1228.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1228.value(row_i))
                    });
                    vals.push(if bhck1286.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1286.value(row_i))
                    });
                    vals.push(if bhck1287.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1287.value(row_i))
                    });
                    vals.push(if bhck1288.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1288.value(row_i))
                    });
                    vals.push(if bhck1289.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1289.value(row_i))
                    });
                    vals.push(if bhck1290.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1290.value(row_i))
                    });
                    vals.push(if bhck1291.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1291.value(row_i))
                    });
                    vals.push(if bhck1292.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1292.value(row_i))
                    });
                    vals.push(if bhck1293.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1293.value(row_i))
                    });
                    vals.push(if bhck1294.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1294.value(row_i))
                    });
                    vals.push(if bhck1295.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1295.value(row_i))
                    });
                    vals.push(if bhck1296.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1296.value(row_i))
                    });
                    vals.push(if bhck1297.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1297.value(row_i))
                    });
                    vals.push(if bhck1298.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1298.value(row_i))
                    });
                    vals.push(if bhck1350.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1350.value(row_i))
                    });
                    vals.push(if bhck1410.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1410.value(row_i))
                    });
                    vals.push(if bhck1421.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck1421.value(row_i))
                    });
                    vals.push(if bhck1422.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1422.value(row_i))
                    });
                    vals.push(if bhck1423.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1423.value(row_i))
                    });
                    vals.push(if bhck1545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1545.value(row_i))
                    });
                    vals.push(if bhck1563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1563.value(row_i))
                    });
                    vals.push(if bhck1564.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1564.value(row_i))
                    });
                    vals.push(if bhck1583.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1583.value(row_i))
                    });
                    vals.push(if bhck1590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1590.value(row_i))
                    });
                    vals.push(if bhck1594.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1594.value(row_i))
                    });
                    vals.push(if bhck1597.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1597.value(row_i))
                    });
                    vals.push(if bhck1606.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1606.value(row_i))
                    });
                    vals.push(if bhck1607.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1607.value(row_i))
                    });
                    vals.push(if bhck1608.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1608.value(row_i))
                    });
                    vals.push(if bhck1611.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1611.value(row_i))
                    });
                    vals.push(if bhck1612.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1612.value(row_i))
                    });
                    vals.push(if bhck1613.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1613.value(row_i))
                    });
                    vals.push(if bhck1615.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1615.value(row_i))
                    });
                    vals.push(if bhck1616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1616.value(row_i))
                    });
                    vals.push(if bhck1635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1635.value(row_i))
                    });
                    vals.push(if bhck1636.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1636.value(row_i))
                    });
                    vals.push(if bhck1638.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1638.value(row_i))
                    });
                    vals.push(if bhck1639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1639.value(row_i))
                    });
                    vals.push(if bhck1651.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1651.value(row_i))
                    });
                    vals.push(if bhck1698.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1698.value(row_i))
                    });
                    vals.push(if bhck1699.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1699.value(row_i))
                    });
                    vals.push(if bhck1701.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1701.value(row_i))
                    });
                    vals.push(if bhck1702.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1702.value(row_i))
                    });
                    vals.push(if bhck1703.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1703.value(row_i))
                    });
                    vals.push(if bhck1705.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1705.value(row_i))
                    });
                    vals.push(if bhck1706.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1706.value(row_i))
                    });
                    vals.push(if bhck1707.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1707.value(row_i))
                    });
                    vals.push(if bhck1709.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1709.value(row_i))
                    });
                    vals.push(if bhck1710.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1710.value(row_i))
                    });
                    vals.push(if bhck1711.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1711.value(row_i))
                    });
                    vals.push(if bhck1713.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1713.value(row_i))
                    });
                    vals.push(if bhck1714.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1714.value(row_i))
                    });
                    vals.push(if bhck1715.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1715.value(row_i))
                    });
                    vals.push(if bhck1716.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1716.value(row_i))
                    });
                    vals.push(if bhck1717.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1717.value(row_i))
                    });
                    vals.push(if bhck1718.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1718.value(row_i))
                    });
                    vals.push(if bhck1719.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1719.value(row_i))
                    });
                    vals.push(if bhck1727.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1727.value(row_i))
                    });
                    vals.push(if bhck1731.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1731.value(row_i))
                    });
                    vals.push(if bhck1732.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1732.value(row_i))
                    });
                    vals.push(if bhck1733.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1733.value(row_i))
                    });
                    vals.push(if bhck1734.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1734.value(row_i))
                    });
                    vals.push(if bhck1735.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1735.value(row_i))
                    });
                    vals.push(if bhck1736.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1736.value(row_i))
                    });
                    vals.push(if bhck1737.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1737.value(row_i))
                    });
                    vals.push(if bhck1738.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1738.value(row_i))
                    });
                    vals.push(if bhck1739.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1739.value(row_i))
                    });
                    vals.push(if bhck1741.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1741.value(row_i))
                    });
                    vals.push(if bhck1742.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1742.value(row_i))
                    });
                    vals.push(if bhck1743.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1743.value(row_i))
                    });
                    vals.push(if bhck1744.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1744.value(row_i))
                    });
                    vals.push(if bhck1746.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1746.value(row_i))
                    });
                    vals.push(if bhck1752.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1752.value(row_i))
                    });
                    vals.push(if bhck1753.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1753.value(row_i))
                    });
                    vals.push(if bhck1754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1754.value(row_i))
                    });
                    vals.push(if bhck1755.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1755.value(row_i))
                    });
                    vals.push(if bhck1763.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1763.value(row_i))
                    });
                    vals.push(if bhck1764.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1764.value(row_i))
                    });
                    vals.push(if bhck1766.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1766.value(row_i))
                    });
                    vals.push(if bhck1773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1773.value(row_i))
                    });
                    vals.push(if bhck1778.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1778.value(row_i))
                    });
                    vals.push(if bhck1912.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1912.value(row_i))
                    });
                    vals.push(if bhck1913.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1913.value(row_i))
                    });
                    vals.push(if bhck1975.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1975.value(row_i))
                    });
                    vals.push(if bhck2008.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2008.value(row_i))
                    });
                    vals.push(if bhck2011.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2011.value(row_i))
                    });
                    vals.push(if bhck2081.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2081.value(row_i))
                    });
                    vals.push(if bhck2130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2130.value(row_i))
                    });
                    vals.push(if bhck2143.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2143.value(row_i))
                    });
                    vals.push(if bhck2148.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2148.value(row_i))
                    });
                    vals.push(if bhck2150.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2150.value(row_i))
                    });
                    vals.push(if bhck2155.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2155.value(row_i))
                    });
                    vals.push(if bhck2160.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2160.value(row_i))
                    });
                    vals.push(if bhck2165.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2165.value(row_i))
                    });
                    vals.push(if bhck2168.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2168.value(row_i))
                    });
                    vals.push(if bhck2182.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2182.value(row_i))
                    });
                    vals.push(if bhck2183.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2183.value(row_i))
                    });
                    vals.push(if bhck2309.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2309.value(row_i))
                    });
                    vals.push(if bhck2332.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2332.value(row_i))
                    });
                    vals.push(if bhck2333.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2333.value(row_i))
                    });
                    vals.push(if bhck2745.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2745.value(row_i))
                    });
                    vals.push(if bhck2746.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2746.value(row_i))
                    });
                    vals.push(if bhck2747.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2747.value(row_i))
                    });
                    vals.push(if bhck2748.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2748.value(row_i))
                    });
                    vals.push(if bhck2749.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2749.value(row_i))
                    });
                    vals.push(if bhck2750.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2750.value(row_i))
                    });
                    vals.push(if bhck2757.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2757.value(row_i))
                    });
                    vals.push(if bhck2759.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2759.value(row_i))
                    });
                    vals.push(if bhck2769.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2769.value(row_i))
                    });
                    vals.push(if bhck2771.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2771.value(row_i))
                    });
                    vals.push(if bhck2800.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2800.value(row_i))
                    });
                    vals.push(if bhck2920.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2920.value(row_i))
                    });
                    vals.push(if bhck3000.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3000.value(row_i))
                    });
                    vals.push(if bhck3049.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3049.value(row_i))
                    });
                    vals.push(if bhck3123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3123.value(row_i))
                    });
                    vals.push(if bhck3124.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3124.value(row_i))
                    });
                    vals.push(if bhck3128.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3128.value(row_i))
                    });
                    vals.push(if bhck3153.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3153.value(row_i))
                    });
                    vals.push(if bhck3163.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3163.value(row_i))
                    });
                    vals.push(if bhck3164.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3164.value(row_i))
                    });
                    vals.push(if bhck3190.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3190.value(row_i))
                    });
                    vals.push(if bhck3197.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3197.value(row_i))
                    });
                    vals.push(if bhck3215.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3215.value(row_i))
                    });
                    vals.push(if bhck3216.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3216.value(row_i))
                    });
                    vals.push(if bhck3217.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3217.value(row_i))
                    });
                    vals.push(if bhck3230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3230.value(row_i))
                    });
                    vals.push(if bhck3284.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3284.value(row_i))
                    });
                    vals.push(if bhck3296.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3296.value(row_i))
                    });
                    vals.push(if bhck3297.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3297.value(row_i))
                    });
                    vals.push(if bhck3298.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3298.value(row_i))
                    });
                    vals.push(if bhck3409.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3409.value(row_i))
                    });
                    vals.push(if bhck3411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3411.value(row_i))
                    });
                    vals.push(if bhck3430.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3430.value(row_i))
                    });
                    vals.push(if bhck3434.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3434.value(row_i))
                    });
                    vals.push(if bhck3435.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3435.value(row_i))
                    });
                    vals.push(if bhck3450.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3450.value(row_i))
                    });
                    vals.push(if bhck3451.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck3451.value(row_i))
                    });
                    vals.push(if bhck3452.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck3452.value(row_i))
                    });
                    vals.push(if bhck3492.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3492.value(row_i))
                    });
                    vals.push(if bhck3493.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3493.value(row_i))
                    });
                    vals.push(if bhck3494.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3494.value(row_i))
                    });
                    vals.push(if bhck3495.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3495.value(row_i))
                    });
                    vals.push(if bhck3499.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3499.value(row_i))
                    });
                    vals.push(if bhck3500.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3500.value(row_i))
                    });
                    vals.push(if bhck3501.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3501.value(row_i))
                    });
                    vals.push(if bhck3502.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3502.value(row_i))
                    });
                    vals.push(if bhck3503.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3503.value(row_i))
                    });
                    vals.push(if bhck3504.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3504.value(row_i))
                    });
                    vals.push(if bhck3505.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3505.value(row_i))
                    });
                    vals.push(if bhck3506.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3506.value(row_i))
                    });
                    vals.push(if bhck3507.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3507.value(row_i))
                    });
                    vals.push(if bhck3508.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3508.value(row_i))
                    });
                    vals.push(if bhck3522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck3522.value(row_i))
                    });
                    vals.push(if bhck3528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3528.value(row_i))
                    });
                    vals.push(if bhck3529.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3529.value(row_i))
                    });
                    vals.push(if bhck3530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3530.value(row_i))
                    });
                    vals.push(if bhck3541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3541.value(row_i))
                    });
                    vals.push(if bhck3546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3546.value(row_i))
                    });
                    vals.push(if bhck3571.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3571.value(row_i))
                    });
                    vals.push(if bhck3572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3572.value(row_i))
                    });
                    vals.push(if bhck3574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3574.value(row_i))
                    });
                    vals.push(if bhck3576.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3576.value(row_i))
                    });
                    vals.push(if bhck3578.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3578.value(row_i))
                    });
                    vals.push(if bhck3580.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3580.value(row_i))
                    });
                    vals.push(if bhck3581.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3581.value(row_i))
                    });
                    vals.push(if bhck3582.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3582.value(row_i))
                    });
                    vals.push(if bhck3584.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3584.value(row_i))
                    });
                    vals.push(if bhck3588.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3588.value(row_i))
                    });
                    vals.push(if bhck3590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3590.value(row_i))
                    });
                    vals.push(if bhck3656.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3656.value(row_i))
                    });
                    vals.push(if bhck3806.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3806.value(row_i))
                    });
                    vals.push(if bhck3809.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3809.value(row_i))
                    });
                    vals.push(if bhck3812.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3812.value(row_i))
                    });
                    vals.push(if bhck3816.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3816.value(row_i))
                    });
                    vals.push(if bhck3820.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3820.value(row_i))
                    });
                    vals.push(if bhck3822.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3822.value(row_i))
                    });
                    vals.push(if bhck3826.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3826.value(row_i))
                    });
                    vals.push(if bhck3836.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3836.value(row_i))
                    });
                    vals.push(if bhck3837.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3837.value(row_i))
                    });
                    vals.push(if bhck4010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4010.value(row_i))
                    });
                    vals.push(if bhck4020.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4020.value(row_i))
                    });
                    vals.push(if bhck4027.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4027.value(row_i))
                    });
                    vals.push(if bhck4042.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4042.value(row_i))
                    });
                    vals.push(if bhck4059.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4059.value(row_i))
                    });
                    vals.push(if bhck4060.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4060.value(row_i))
                    });
                    vals.push(if bhck4065.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4065.value(row_i))
                    });
                    vals.push(if bhck4069.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4069.value(row_i))
                    });
                    vals.push(if bhck4070.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4070.value(row_i))
                    });
                    vals.push(if bhck4074.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4074.value(row_i))
                    });
                    vals.push(if bhck4078.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4078.value(row_i))
                    });
                    vals.push(if bhck4092.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4092.value(row_i))
                    });
                    vals.push(if bhck4105.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4105.value(row_i))
                    });
                    vals.push(if bhck4106.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4106.value(row_i))
                    });
                    vals.push(if bhck4115.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4115.value(row_i))
                    });
                    vals.push(if bhck4136.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4136.value(row_i))
                    });
                    vals.push(if bhck4141.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4141.value(row_i))
                    });
                    vals.push(if bhck4146.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4146.value(row_i))
                    });
                    vals.push(if bhck4150.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4150.value(row_i))
                    });
                    vals.push(if bhck4172.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4172.value(row_i))
                    });
                    vals.push(if bhck4180.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4180.value(row_i))
                    });
                    vals.push(if bhck4185.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4185.value(row_i))
                    });
                    vals.push(if bhck4217.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4217.value(row_i))
                    });
                    vals.push(if bhck4219.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4219.value(row_i))
                    });
                    vals.push(if bhck4300.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4300.value(row_i))
                    });
                    vals.push(if bhck4301.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4301.value(row_i))
                    });
                    vals.push(if bhck4302.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4302.value(row_i))
                    });
                    vals.push(if bhck4313.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4313.value(row_i))
                    });
                    vals.push(if bhck4320.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4320.value(row_i))
                    });
                    vals.push(if bhck4336.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4336.value(row_i))
                    });
                    vals.push(if bhck4340.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4340.value(row_i))
                    });
                    vals.push(if bhck4356.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4356.value(row_i))
                    });
                    vals.push(if bhck4393.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4393.value(row_i))
                    });
                    vals.push(if bhck4394.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4394.value(row_i))
                    });
                    vals.push(if bhck4395.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4395.value(row_i))
                    });
                    vals.push(if bhck4396.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4396.value(row_i))
                    });
                    vals.push(if bhck4397.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4397.value(row_i))
                    });
                    vals.push(if bhck4398.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4398.value(row_i))
                    });
                    vals.push(if bhck4399.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4399.value(row_i))
                    });
                    vals.push(if bhck4411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4411.value(row_i))
                    });
                    vals.push(if bhck4412.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4412.value(row_i))
                    });
                    vals.push(if bhck4414.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4414.value(row_i))
                    });
                    vals.push(if bhck4435.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4435.value(row_i))
                    });
                    vals.push(if bhck4436.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4436.value(row_i))
                    });
                    vals.push(if bhck4460.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4460.value(row_i))
                    });
                    vals.push(if bhck4484.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4484.value(row_i))
                    });
                    vals.push(if bhck4503.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4503.value(row_i))
                    });
                    vals.push(if bhck4504.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4504.value(row_i))
                    });
                    vals.push(if bhck4506.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4506.value(row_i))
                    });
                    vals.push(if bhck4507.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4507.value(row_i))
                    });
                    vals.push(if bhck4518.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4518.value(row_i))
                    });
                    vals.push(if bhck4519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4519.value(row_i))
                    });
                    vals.push(if bhck4531.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4531.value(row_i))
                    });
                    vals.push(if bhck4574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4574.value(row_i))
                    });
                    vals.push(if bhck4591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4591.value(row_i))
                    });
                    vals.push(if bhck4592.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4592.value(row_i))
                    });
                    vals.push(if bhck4598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4598.value(row_i))
                    });
                    vals.push(if bhck4635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4635.value(row_i))
                    });
                    vals.push(if bhck4643.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4643.value(row_i))
                    });
                    vals.push(if bhck4644.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4644.value(row_i))
                    });
                    vals.push(if bhck4645.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4645.value(row_i))
                    });
                    vals.push(if bhck4646.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4646.value(row_i))
                    });
                    vals.push(if bhck4651.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4651.value(row_i))
                    });
                    vals.push(if bhck4652.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4652.value(row_i))
                    });
                    vals.push(if bhck4653.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4653.value(row_i))
                    });
                    vals.push(if bhck4654.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4654.value(row_i))
                    });
                    vals.push(if bhck4655.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4655.value(row_i))
                    });
                    vals.push(if bhck4656.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4656.value(row_i))
                    });
                    vals.push(if bhck4657.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4657.value(row_i))
                    });
                    vals.push(if bhck4658.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4658.value(row_i))
                    });
                    vals.push(if bhck4659.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4659.value(row_i))
                    });
                    vals.push(if bhck4776.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4776.value(row_i))
                    });
                    vals.push(if bhck4815.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4815.value(row_i))
                    });
                    vals.push(if bhck4832.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4832.value(row_i))
                    });
                    vals.push(if bhck4833.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4833.value(row_i))
                    });
                    vals.push(if bhck4834.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4834.value(row_i))
                    });
                    vals.push(if bhck5041.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5041.value(row_i))
                    });
                    vals.push(if bhck5043.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5043.value(row_i))
                    });
                    vals.push(if bhck5045.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5045.value(row_i))
                    });
                    vals.push(if bhck5047.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5047.value(row_i))
                    });
                    vals.push(if bhck5310.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5310.value(row_i))
                    });
                    vals.push(if bhck5351.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5351.value(row_i))
                    });
                    vals.push(if bhck5354.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5354.value(row_i))
                    });
                    vals.push(if bhck5355.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5355.value(row_i))
                    });
                    vals.push(if bhck5356.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5356.value(row_i))
                    });
                    vals.push(if bhck5359.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5359.value(row_i))
                    });
                    vals.push(if bhck5360.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5360.value(row_i))
                    });
                    vals.push(if bhck5369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5369.value(row_i))
                    });
                    vals.push(if bhck5377.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5377.value(row_i))
                    });
                    vals.push(if bhck5378.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5378.value(row_i))
                    });
                    vals.push(if bhck5379.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5379.value(row_i))
                    });
                    vals.push(if bhck5380.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5380.value(row_i))
                    });
                    vals.push(if bhck5381.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5381.value(row_i))
                    });
                    vals.push(if bhck5382.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5382.value(row_i))
                    });
                    vals.push(if bhck5383.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck5383.value(row_i))
                    });
                    vals.push(if bhck5384.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5384.value(row_i))
                    });
                    vals.push(if bhck5385.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5385.value(row_i))
                    });
                    vals.push(if bhck5386.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck5386.value(row_i))
                    });
                    vals.push(if bhck5387.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5387.value(row_i))
                    });
                    vals.push(if bhck5388.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5388.value(row_i))
                    });
                    vals.push(if bhck5389.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5389.value(row_i))
                    });
                    vals.push(if bhck5390.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5390.value(row_i))
                    });
                    vals.push(if bhck5391.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5391.value(row_i))
                    });
                    vals.push(if bhck5393.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5393.value(row_i))
                    });
                    vals.push(if bhck5397.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5397.value(row_i))
                    });
                    vals.push(if bhck5398.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5398.value(row_i))
                    });
                    vals.push(if bhck5399.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5399.value(row_i))
                    });
                    vals.push(if bhck5400.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5400.value(row_i))
                    });
                    vals.push(if bhck5401.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5401.value(row_i))
                    });
                    vals.push(if bhck5402.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5402.value(row_i))
                    });
                    vals.push(if bhck5403.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5403.value(row_i))
                    });
                    vals.push(if bhck5409.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5409.value(row_i))
                    });
                    vals.push(if bhck5411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5411.value(row_i))
                    });
                    vals.push(if bhck5413.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5413.value(row_i))
                    });
                    vals.push(if bhck5459.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5459.value(row_i))
                    });
                    vals.push(if bhck5460.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5460.value(row_i))
                    });
                    vals.push(if bhck5461.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5461.value(row_i))
                    });
                    vals.push(if bhck5507.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5507.value(row_i))
                    });
                    vals.push(if bhck5610.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5610.value(row_i))
                    });
                    vals.push(if bhck5612.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5612.value(row_i))
                    });
                    vals.push(if bhck5613.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5613.value(row_i))
                    });
                    vals.push(if bhck5614.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5614.value(row_i))
                    });
                    vals.push(if bhck5615.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5615.value(row_i))
                    });
                    vals.push(if bhck5616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5616.value(row_i))
                    });
                    vals.push(if bhck5617.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5617.value(row_i))
                    });
                    vals.push(if bhck6019.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6019.value(row_i))
                    });
                    vals.push(if bhck6373.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6373.value(row_i))
                    });
                    vals.push(if bhck6416.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6416.value(row_i))
                    });
                    vals.push(if bhck6438.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6438.value(row_i))
                    });
                    vals.push(if bhck6441.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6441.value(row_i))
                    });
                    vals.push(if bhck6442.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6442.value(row_i))
                    });
                    vals.push(if bhck6550.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6550.value(row_i))
                    });
                    vals.push(if bhck6555.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6555.value(row_i))
                    });
                    vals.push(if bhck6556.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6556.value(row_i))
                    });
                    vals.push(if bhck6557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6557.value(row_i))
                    });
                    vals.push(if bhck6558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6558.value(row_i))
                    });
                    vals.push(if bhck6559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6559.value(row_i))
                    });
                    vals.push(if bhck6560.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6560.value(row_i))
                    });
                    vals.push(if bhck6561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6561.value(row_i))
                    });
                    vals.push(if bhck6566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6566.value(row_i))
                    });
                    vals.push(if bhck6572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6572.value(row_i))
                    });
                    vals.push(if bhck6586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6586.value(row_i))
                    });
                    vals.push(if bhck6599.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6599.value(row_i))
                    });
                    vals.push(if bhck6649.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6649.value(row_i))
                    });
                    vals.push(if bhck6669.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck6669.value(row_i))
                    });
                    vals.push(if bhck6688.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6688.value(row_i))
                    });
                    vals.push(if bhck6689.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6689.value(row_i))
                    });
                    vals.push(if bhck6761.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6761.value(row_i))
                    });
                    vals.push(if bhck6765.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6765.value(row_i))
                    });
                    vals.push(if bhck6927.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck6927.value(row_i))
                    });
                    vals.push(if bhck6928.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck6928.value(row_i))
                    });
                    vals.push(if bhck6995.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck6995.value(row_i))
                    });
                    vals.push(if bhck6998.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck6998.value(row_i))
                    });
                    vals.push(if bhck8403.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8403.value(row_i))
                    });
                    vals.push(if bhck8427.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8427.value(row_i))
                    });
                    vals.push(if bhck8428.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8428.value(row_i))
                    });
                    vals.push(if bhck8429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8429.value(row_i))
                    });
                    vals.push(if bhck8430.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8430.value(row_i))
                    });
                    vals.push(if bhck8431.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8431.value(row_i))
                    });
                    vals.push(if bhck8433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8433.value(row_i))
                    });
                    vals.push(if bhck8434.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8434.value(row_i))
                    });
                    vals.push(if bhck8492.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8492.value(row_i))
                    });
                    vals.push(if bhck8493.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8493.value(row_i))
                    });
                    vals.push(if bhck8494.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8494.value(row_i))
                    });
                    vals.push(if bhck8495.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8495.value(row_i))
                    });
                    vals.push(if bhck8496.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8496.value(row_i))
                    });
                    vals.push(if bhck8497.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8497.value(row_i))
                    });
                    vals.push(if bhck8498.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8498.value(row_i))
                    });
                    vals.push(if bhck8499.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8499.value(row_i))
                    });
                    vals.push(if bhck8531.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8531.value(row_i))
                    });
                    vals.push(if bhck8532.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8532.value(row_i))
                    });
                    vals.push(if bhck8533.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8533.value(row_i))
                    });
                    vals.push(if bhck8534.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8534.value(row_i))
                    });
                    vals.push(if bhck8535.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8535.value(row_i))
                    });
                    vals.push(if bhck8536.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8536.value(row_i))
                    });
                    vals.push(if bhck8537.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8537.value(row_i))
                    });
                    vals.push(if bhck8538.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8538.value(row_i))
                    });
                    vals.push(if bhck8539.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8539.value(row_i))
                    });
                    vals.push(if bhck8540.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8540.value(row_i))
                    });
                    vals.push(if bhck8541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8541.value(row_i))
                    });
                    vals.push(if bhck8542.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8542.value(row_i))
                    });
                    vals.push(if bhck8543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8543.value(row_i))
                    });
                    vals.push(if bhck8544.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8544.value(row_i))
                    });
                    vals.push(if bhck8545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8545.value(row_i))
                    });
                    vals.push(if bhck8546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8546.value(row_i))
                    });
                    vals.push(if bhck8547.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8547.value(row_i))
                    });
                    vals.push(if bhck8548.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8548.value(row_i))
                    });
                    vals.push(if bhck8549.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8549.value(row_i))
                    });
                    vals.push(if bhck8550.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8550.value(row_i))
                    });
                    vals.push(if bhck8557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8557.value(row_i))
                    });
                    vals.push(if bhck8558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8558.value(row_i))
                    });
                    vals.push(if bhck8559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8559.value(row_i))
                    });
                    vals.push(if bhck8560.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8560.value(row_i))
                    });
                    vals.push(if bhck8561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8561.value(row_i))
                    });
                    vals.push(if bhck8562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8562.value(row_i))
                    });
                    vals.push(if bhck8563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8563.value(row_i))
                    });
                    vals.push(if bhck8564.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8564.value(row_i))
                    });
                    vals.push(if bhck8565.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8565.value(row_i))
                    });
                    vals.push(if bhck8566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8566.value(row_i))
                    });
                    vals.push(if bhck8567.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8567.value(row_i))
                    });
                    vals.push(if bhck8693.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8693.value(row_i))
                    });
                    vals.push(if bhck8694.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8694.value(row_i))
                    });
                    vals.push(if bhck8695.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8695.value(row_i))
                    });
                    vals.push(if bhck8696.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8696.value(row_i))
                    });
                    vals.push(if bhck8697.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8697.value(row_i))
                    });
                    vals.push(if bhck8698.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8698.value(row_i))
                    });
                    vals.push(if bhck8699.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8699.value(row_i))
                    });
                    vals.push(if bhck8700.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8700.value(row_i))
                    });
                    vals.push(if bhck8719.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8719.value(row_i))
                    });
                    vals.push(if bhck8720.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8720.value(row_i))
                    });
                    vals.push(if bhck8733.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8733.value(row_i))
                    });
                    vals.push(if bhck8734.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8734.value(row_i))
                    });
                    vals.push(if bhck8735.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8735.value(row_i))
                    });
                    vals.push(if bhck8736.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8736.value(row_i))
                    });
                    vals.push(if bhck8737.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8737.value(row_i))
                    });
                    vals.push(if bhck8738.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8738.value(row_i))
                    });
                    vals.push(if bhck8739.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8739.value(row_i))
                    });
                    vals.push(if bhck8740.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8740.value(row_i))
                    });
                    vals.push(if bhck8741.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8741.value(row_i))
                    });
                    vals.push(if bhck8742.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8742.value(row_i))
                    });
                    vals.push(if bhck8743.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8743.value(row_i))
                    });
                    vals.push(if bhck8744.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8744.value(row_i))
                    });
                    vals.push(if bhck8745.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8745.value(row_i))
                    });
                    vals.push(if bhck8746.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8746.value(row_i))
                    });
                    vals.push(if bhck8747.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8747.value(row_i))
                    });
                    vals.push(if bhck8748.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8748.value(row_i))
                    });
                    vals.push(if bhck8749.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8749.value(row_i))
                    });
                    vals.push(if bhck8750.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8750.value(row_i))
                    });
                    vals.push(if bhck8751.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8751.value(row_i))
                    });
                    vals.push(if bhck8752.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8752.value(row_i))
                    });
                    vals.push(if bhck8753.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8753.value(row_i))
                    });
                    vals.push(if bhck8754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8754.value(row_i))
                    });
                    vals.push(if bhck8755.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8755.value(row_i))
                    });
                    vals.push(if bhck8756.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8756.value(row_i))
                    });
                    vals.push(if bhck8757.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8757.value(row_i))
                    });
                    vals.push(if bhck8758.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8758.value(row_i))
                    });
                    vals.push(if bhck8759.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8759.value(row_i))
                    });
                    vals.push(if bhck8760.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8760.value(row_i))
                    });
                    vals.push(if bhck8761.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8761.value(row_i))
                    });
                    vals.push(if bhck8762.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8762.value(row_i))
                    });
                    vals.push(if bhck8763.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8763.value(row_i))
                    });
                    vals.push(if bhck8764.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8764.value(row_i))
                    });
                    vals.push(if bhck8766.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8766.value(row_i))
                    });
                    vals.push(if bhck8767.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8767.value(row_i))
                    });
                    vals.push(if bhck8769.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8769.value(row_i))
                    });
                    vals.push(if bhck8770.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8770.value(row_i))
                    });
                    vals.push(if bhck8771.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8771.value(row_i))
                    });
                    vals.push(if bhck8772.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8772.value(row_i))
                    });
                    vals.push(if bhck8773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8773.value(row_i))
                    });
                    vals.push(if bhck8774.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8774.value(row_i))
                    });
                    vals.push(if bhck8775.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8775.value(row_i))
                    });
                    vals.push(if bhck8776.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8776.value(row_i))
                    });
                    vals.push(if bhck8777.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8777.value(row_i))
                    });
                    vals.push(if bhck8778.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8778.value(row_i))
                    });
                    vals.push(if bhck8779.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8779.value(row_i))
                    });
                    vals.push(if bhck8782.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8782.value(row_i))
                    });
                    vals.push(if bhck8783.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8783.value(row_i))
                    });
                    vals.push(if bhck8843.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8843.value(row_i))
                    });
                    vals.push(if bhcka000.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka000.value(row_i))
                    });
                    vals.push(if bhcka001.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka001.value(row_i))
                    });
                    vals.push(if bhcka002.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka002.value(row_i))
                    });
                    vals.push(if bhcka130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka130.value(row_i))
                    });
                    vals.push(if bhcka221.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka221.value(row_i))
                    });
                    vals.push(if bhcka222.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka222.value(row_i))
                    });
                    vals.push(if bhcka224.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka224.value(row_i))
                    });
                    vals.push(if bhcka250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka250.value(row_i))
                    });
                    vals.push(if bhcka251.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka251.value(row_i))
                    });
                    vals.push(if bhcka506.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka506.value(row_i))
                    });
                    vals.push(if bhcka507.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka507.value(row_i))
                    });
                    vals.push(if bhcka510.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka510.value(row_i))
                    });
                    vals.push(if bhcka511.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka511.value(row_i))
                    });
                    vals.push(if bhcka512.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka512.value(row_i))
                    });
                    vals.push(if bhcka517.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka517.value(row_i))
                    });
                    vals.push(if bhcka518.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka518.value(row_i))
                    });
                    vals.push(if bhcka519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka519.value(row_i))
                    });
                    vals.push(if bhcka520.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka520.value(row_i))
                    });
                    vals.push(if bhcka521.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka521.value(row_i))
                    });
                    vals.push(if bhcka522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka522.value(row_i))
                    });
                    vals.push(if bhcka523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka523.value(row_i))
                    });
                    vals.push(if bhcka524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka524.value(row_i))
                    });
                    vals.push(if bhcka525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka525.value(row_i))
                    });
                    vals.push(if bhcka530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka530.value(row_i))
                    });
                    vals.push(if bhcka534.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka534.value(row_i))
                    });
                    vals.push(if bhcka535.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka535.value(row_i))
                    });
                    vals.push(if bhckb026.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb026.value(row_i))
                    });
                    vals.push(if bhckb029.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb029.value(row_i))
                    });
                    vals.push(if bhckb030.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb030.value(row_i))
                    });
                    vals.push(if bhckb032.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb032.value(row_i))
                    });
                    vals.push(if bhckb035.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb035.value(row_i))
                    });
                    vals.push(if bhckb036.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb036.value(row_i))
                    });
                    vals.push(if bhckb039.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb039.value(row_i))
                    });
                    vals.push(if bhckb040.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb040.value(row_i))
                    });
                    vals.push(if bhckb044.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb044.value(row_i))
                    });
                    vals.push(if bhckb045.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb045.value(row_i))
                    });
                    vals.push(if bhckb047.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb047.value(row_i))
                    });
                    vals.push(if bhckb050.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb050.value(row_i))
                    });
                    vals.push(if bhckb051.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb051.value(row_i))
                    });
                    vals.push(if bhckb054.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb054.value(row_i))
                    });
                    vals.push(if bhckb055.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb055.value(row_i))
                    });
                    vals.push(if bhckb077.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb077.value(row_i))
                    });
                    vals.push(if bhckb488.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb488.value(row_i))
                    });
                    vals.push(if bhckb489.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb489.value(row_i))
                    });
                    vals.push(if bhckb490.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb490.value(row_i))
                    });
                    vals.push(if bhckb492.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb492.value(row_i))
                    });
                    vals.push(if bhckb493.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb493.value(row_i))
                    });
                    vals.push(if bhckb494.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb494.value(row_i))
                    });
                    vals.push(if bhckb496.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb496.value(row_i))
                    });
                    vals.push(if bhckb497.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb497.value(row_i))
                    });
                    vals.push(if bhckb500.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb500.value(row_i))
                    });
                    vals.push(if bhckb501.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb501.value(row_i))
                    });
                    vals.push(if bhckb502.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb502.value(row_i))
                    });
                    vals.push(if bhckb508.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb508.value(row_i))
                    });
                    vals.push(if bhckb511.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb511.value(row_i))
                    });
                    vals.push(if bhckb512.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb512.value(row_i))
                    });
                    vals.push(if bhckb514.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb514.value(row_i))
                    });
                    vals.push(if bhckb516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb516.value(row_i))
                    });
                    vals.push(if bhckb522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb522.value(row_i))
                    });
                    vals.push(if bhckb528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb528.value(row_i))
                    });
                    vals.push(if bhckb529.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb529.value(row_i))
                    });
                    vals.push(if bhckb530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb530.value(row_i))
                    });
                    vals.push(if bhckb538.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb538.value(row_i))
                    });
                    vals.push(if bhckb539.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb539.value(row_i))
                    });
                    vals.push(if bhckb546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb546.value(row_i))
                    });
                    vals.push(if bhckb556.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb556.value(row_i))
                    });
                    vals.push(if bhckb557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb557.value(row_i))
                    });
                    vals.push(if bhckb559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb559.value(row_i))
                    });
                    vals.push(if bhckb560.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb560.value(row_i))
                    });
                    vals.push(if bhckb569.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb569.value(row_i))
                    });
                    vals.push(if bhckb570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb570.value(row_i))
                    });
                    vals.push(if bhckb572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb572.value(row_i))
                    });
                    vals.push(if bhckb573.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb573.value(row_i))
                    });
                    vals.push(if bhckb574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb574.value(row_i))
                    });
                    vals.push(if bhckb575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb575.value(row_i))
                    });
                    vals.push(if bhckb576.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb576.value(row_i))
                    });
                    vals.push(if bhckb577.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb577.value(row_i))
                    });
                    vals.push(if bhckb578.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb578.value(row_i))
                    });
                    vals.push(if bhckb579.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb579.value(row_i))
                    });
                    vals.push(if bhckb580.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb580.value(row_i))
                    });
                    vals.push(if bhckb588.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb588.value(row_i))
                    });
                    vals.push(if bhckb590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb590.value(row_i))
                    });
                    vals.push(if bhckb591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb591.value(row_i))
                    });
                    vals.push(if bhckb592.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb592.value(row_i))
                    });
                    vals.push(if bhckb593.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb593.value(row_i))
                    });
                    vals.push(if bhckb594.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb594.value(row_i))
                    });
                    vals.push(if bhckb595.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb595.value(row_i))
                    });
                    vals.push(if bhckb596.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb596.value(row_i))
                    });
                    vals.push(if bhckb639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb639.value(row_i))
                    });
                    vals.push(if bhckb675.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb675.value(row_i))
                    });
                    vals.push(if bhckb681.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb681.value(row_i))
                    });
                    vals.push(if bhckb747.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb747.value(row_i))
                    });
                    vals.push(if bhckb748.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb748.value(row_i))
                    });
                    vals.push(if bhckb749.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb749.value(row_i))
                    });
                    vals.push(if bhckb750.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb750.value(row_i))
                    });
                    vals.push(if bhckb751.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb751.value(row_i))
                    });
                    vals.push(if bhckb752.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb752.value(row_i))
                    });
                    vals.push(if bhckb753.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb753.value(row_i))
                    });
                    vals.push(if bhckb761.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb761.value(row_i))
                    });
                    vals.push(if bhckb762.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb762.value(row_i))
                    });
                    vals.push(if bhckb763.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb763.value(row_i))
                    });
                    vals.push(if bhckb770.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb770.value(row_i))
                    });
                    vals.push(if bhckb771.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb771.value(row_i))
                    });
                    vals.push(if bhckb772.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb772.value(row_i))
                    });
                    vals.push(if bhckb776.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb776.value(row_i))
                    });
                    vals.push(if bhckb777.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb777.value(row_i))
                    });
                    vals.push(if bhckb778.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb778.value(row_i))
                    });
                    vals.push(if bhckb779.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb779.value(row_i))
                    });
                    vals.push(if bhckb780.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb780.value(row_i))
                    });
                    vals.push(if bhckb781.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb781.value(row_i))
                    });
                    vals.push(if bhckb782.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb782.value(row_i))
                    });
                    vals.push(if bhckb790.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb790.value(row_i))
                    });
                    vals.push(if bhckb791.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb791.value(row_i))
                    });
                    vals.push(if bhckb792.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb792.value(row_i))
                    });
                    vals.push(if bhckb793.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb793.value(row_i))
                    });
                    vals.push(if bhckb794.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb794.value(row_i))
                    });
                    vals.push(if bhckb795.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb795.value(row_i))
                    });
                    vals.push(if bhckb796.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb796.value(row_i))
                    });
                    vals.push(if bhckb797.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb797.value(row_i))
                    });
                    vals.push(if bhckb798.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb798.value(row_i))
                    });
                    vals.push(if bhckb799.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb799.value(row_i))
                    });
                    vals.push(if bhckb800.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb800.value(row_i))
                    });
                    vals.push(if bhckb801.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb801.value(row_i))
                    });
                    vals.push(if bhckb802.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb802.value(row_i))
                    });
                    vals.push(if bhckb803.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb803.value(row_i))
                    });
                    vals.push(if bhckb806.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb806.value(row_i))
                    });
                    vals.push(if bhckb807.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb807.value(row_i))
                    });
                    vals.push(if bhckb837.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb837.value(row_i))
                    });
                    vals.push(if bhckb838.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb838.value(row_i))
                    });
                    vals.push(if bhckb839.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb839.value(row_i))
                    });
                    vals.push(if bhckb840.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb840.value(row_i))
                    });
                    vals.push(if bhckb841.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb841.value(row_i))
                    });
                    vals.push(if bhckb842.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb842.value(row_i))
                    });
                    vals.push(if bhckb843.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb843.value(row_i))
                    });
                    vals.push(if bhckb844.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb844.value(row_i))
                    });
                    vals.push(if bhckb845.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb845.value(row_i))
                    });
                    vals.push(if bhckb846.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb846.value(row_i))
                    });
                    vals.push(if bhckb847.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb847.value(row_i))
                    });
                    vals.push(if bhckb848.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb848.value(row_i))
                    });
                    vals.push(if bhckb849.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb849.value(row_i))
                    });
                    vals.push(if bhckb850.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb850.value(row_i))
                    });
                    vals.push(if bhckb851.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb851.value(row_i))
                    });
                    vals.push(if bhckb852.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb852.value(row_i))
                    });
                    vals.push(if bhckb853.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb853.value(row_i))
                    });
                    vals.push(if bhckb854.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb854.value(row_i))
                    });
                    vals.push(if bhckb855.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb855.value(row_i))
                    });
                    vals.push(if bhckb856.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb856.value(row_i))
                    });
                    vals.push(if bhckb857.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb857.value(row_i))
                    });
                    vals.push(if bhckb858.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb858.value(row_i))
                    });
                    vals.push(if bhckb859.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb859.value(row_i))
                    });
                    vals.push(if bhckb860.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb860.value(row_i))
                    });
                    vals.push(if bhckb861.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb861.value(row_i))
                    });
                    vals.push(if bhckb983.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb983.value(row_i))
                    });
                    vals.push(if bhckb984.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb984.value(row_i))
                    });
                    vals.push(if bhckb985.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb985.value(row_i))
                    });
                    vals.push(if bhckb986.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckb986.value(row_i))
                    });
                    vals.push(if bhckb988.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb988.value(row_i))
                    });
                    vals.push(if bhckb990.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb990.value(row_i))
                    });
                    vals.push(if bhckb991.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb991.value(row_i))
                    });
                    vals.push(if bhckb992.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb992.value(row_i))
                    });
                    vals.push(if bhckb994.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb994.value(row_i))
                    });
                    vals.push(if bhckb996.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb996.value(row_i))
                    });
                    vals.push(if bhckb998.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb998.value(row_i))
                    });
                    vals.push(if bhckc009.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc009.value(row_i))
                    });
                    vals.push(if bhckc013.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc013.value(row_i))
                    });
                    vals.push(if bhckc014.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc014.value(row_i))
                    });
                    vals.push(if bhckc016.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc016.value(row_i))
                    });
                    vals.push(if bhckc017.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc017.value(row_i))
                    });
                    vals.push(if bhckc050.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckc050.value(row_i))
                    });
                    vals.push(if bhckc079.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc079.value(row_i))
                    });
                    vals.push(if bhckc159.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc159.value(row_i))
                    });
                    vals.push(if bhckc160.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc160.value(row_i))
                    });
                    vals.push(if bhckc161.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc161.value(row_i))
                    });
                    vals.push(if bhckc216.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc216.value(row_i))
                    });
                    vals.push(if bhckc219.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc219.value(row_i))
                    });
                    vals.push(if bhckc220.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc220.value(row_i))
                    });
                    vals.push(if bhckc221.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc221.value(row_i))
                    });
                    vals.push(if bhckc222.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc222.value(row_i))
                    });
                    vals.push(if bhckc225.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc225.value(row_i))
                    });
                    vals.push(if bhckc226.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc226.value(row_i))
                    });
                    vals.push(if bhckc229.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc229.value(row_i))
                    });
                    vals.push(if bhckc230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc230.value(row_i))
                    });
                    vals.push(if bhckc231.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc231.value(row_i))
                    });
                    vals.push(if bhckc232.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc232.value(row_i))
                    });
                    vals.push(if bhckc233.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc233.value(row_i))
                    });
                    vals.push(if bhckc234.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc234.value(row_i))
                    });
                    vals.push(if bhckc235.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc235.value(row_i))
                    });
                    vals.push(if bhckc236.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc236.value(row_i))
                    });
                    vals.push(if bhckc237.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc237.value(row_i))
                    });
                    vals.push(if bhckc238.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc238.value(row_i))
                    });
                    vals.push(if bhckc239.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc239.value(row_i))
                    });
                    vals.push(if bhckc240.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc240.value(row_i))
                    });
                    vals.push(if bhckc241.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc241.value(row_i))
                    });
                    vals.push(if bhckc243.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc243.value(row_i))
                    });
                    vals.push(if bhckc246.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc246.value(row_i))
                    });
                    vals.push(if bhckc250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc250.value(row_i))
                    });
                    vals.push(if bhckc251.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc251.value(row_i))
                    });
                    vals.push(if bhckc252.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc252.value(row_i))
                    });
                    vals.push(if bhckc253.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc253.value(row_i))
                    });
                    vals.push(if bhckc386.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc386.value(row_i))
                    });
                    vals.push(if bhckc387.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc387.value(row_i))
                    });
                    vals.push(if bhckc390.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc390.value(row_i))
                    });
                    vals.push(if bhckc410.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc410.value(row_i))
                    });
                    vals.push(if bhckc411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc411.value(row_i))
                    });
                    vals.push(if bhckc435.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc435.value(row_i))
                    });
                    vals.push(if bhckc447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc447.value(row_i))
                    });
                    vals.push(if bhckc498.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc498.value(row_i))
                    });
                    vals.push(if bhckc700.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc700.value(row_i))
                    });
                    vals.push(if bhckc701.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc701.value(row_i))
                    });
                    vals.push(if bhckc781.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc781.value(row_i))
                    });
                    vals.push(if bhckc880.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc880.value(row_i))
                    });
                    vals.push(if bhckc884.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc884.value(row_i))
                    });
                    vals.push(if bhckc886.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc886.value(row_i))
                    });
                    vals.push(if bhckc887.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc887.value(row_i))
                    });
                    vals.push(if bhckc888.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc888.value(row_i))
                    });
                    vals.push(if bhckc889.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc889.value(row_i))
                    });
                    vals.push(if bhckc890.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc890.value(row_i))
                    });
                    vals.push(if bhckc891.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc891.value(row_i))
                    });
                    vals.push(if bhckc892.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc892.value(row_i))
                    });
                    vals.push(if bhckc893.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc893.value(row_i))
                    });
                    vals.push(if bhckc894.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc894.value(row_i))
                    });
                    vals.push(if bhckc895.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc895.value(row_i))
                    });
                    vals.push(if bhckc896.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc896.value(row_i))
                    });
                    vals.push(if bhckc897.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc897.value(row_i))
                    });
                    vals.push(if bhckc898.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc898.value(row_i))
                    });
                    vals.push(if bhckc968.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc968.value(row_i))
                    });
                    vals.push(if bhckc969.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc969.value(row_i))
                    });
                    vals.push(if bhckc970.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc970.value(row_i))
                    });
                    vals.push(if bhckc971.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc971.value(row_i))
                    });
                    vals.push(if bhckc972.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc972.value(row_i))
                    });
                    vals.push(if bhckc973.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc973.value(row_i))
                    });
                    vals.push(if bhckc974.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc974.value(row_i))
                    });
                    vals.push(if bhckc975.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc975.value(row_i))
                    });
                    vals.push(if bhckc980.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc980.value(row_i))
                    });
                    vals.push(if bhckc981.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc981.value(row_i))
                    });
                    vals.push(if bhckc982.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc982.value(row_i))
                    });
                    vals.push(if bhckc983.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc983.value(row_i))
                    });
                    vals.push(if bhckc984.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc984.value(row_i))
                    });
                    vals.push(if bhckc985.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc985.value(row_i))
                    });
                    vals.push(if bhckc988.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc988.value(row_i))
                    });
                    vals.push(if bhckc989.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc989.value(row_i))
                    });
                    vals.push(if bhckd958.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd958.value(row_i))
                    });
                    vals.push(if bhckd959.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd959.value(row_i))
                    });
                    vals.push(if bhckd960.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd960.value(row_i))
                    });
                    vals.push(if bhckd962.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd962.value(row_i))
                    });
                    vals.push(if bhckd963.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd963.value(row_i))
                    });
                    vals.push(if bhckd964.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd964.value(row_i))
                    });
                    vals.push(if bhckd965.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd965.value(row_i))
                    });
                    vals.push(if bhckd967.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd967.value(row_i))
                    });
                    vals.push(if bhckd968.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd968.value(row_i))
                    });
                    vals.push(if bhckd969.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd969.value(row_i))
                    });
                    vals.push(if bhckd970.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd970.value(row_i))
                    });
                    vals.push(if bhckd971.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd971.value(row_i))
                    });
                    vals.push(if bhckd972.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd972.value(row_i))
                    });
                    vals.push(if bhckd973.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd973.value(row_i))
                    });
                    vals.push(if bhckd974.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd974.value(row_i))
                    });
                    vals.push(if bhckd982.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd982.value(row_i))
                    });
                    vals.push(if bhckd983.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd983.value(row_i))
                    });
                    vals.push(if bhckd984.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd984.value(row_i))
                    });
                    vals.push(if bhckd985.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd985.value(row_i))
                    });
                    vals.push(if bhckd991.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd991.value(row_i))
                    });
                    vals.push(if bhckd992.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd992.value(row_i))
                    });
                    vals.push(if bhckd993.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd993.value(row_i))
                    });
                    vals.push(if bhckd994.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd994.value(row_i))
                    });
                    vals.push(if bhckd995.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd995.value(row_i))
                    });
                    vals.push(if bhckd996.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd996.value(row_i))
                    });
                    vals.push(if bhckf031.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf031.value(row_i))
                    });
                    vals.push(if bhckf070.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf070.value(row_i))
                    });
                    vals.push(if bhckf071.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf071.value(row_i))
                    });
                    vals.push(if bhckf072.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf072.value(row_i))
                    });
                    vals.push(if bhckf073.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf073.value(row_i))
                    });
                    vals.push(if bhckf158.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf158.value(row_i))
                    });
                    vals.push(if bhckf159.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf159.value(row_i))
                    });
                    vals.push(if bhckf160.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf160.value(row_i))
                    });
                    vals.push(if bhckf161.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf161.value(row_i))
                    });
                    vals.push(if bhckf162.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf162.value(row_i))
                    });
                    vals.push(if bhckf163.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf163.value(row_i))
                    });
                    vals.push(if bhckf164.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf164.value(row_i))
                    });
                    vals.push(if bhckf165.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf165.value(row_i))
                    });
                    vals.push(if bhckf166.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf166.value(row_i))
                    });
                    vals.push(if bhckf167.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf167.value(row_i))
                    });
                    vals.push(if bhckf168.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf168.value(row_i))
                    });
                    vals.push(if bhckf169.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf169.value(row_i))
                    });
                    vals.push(if bhckf170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf170.value(row_i))
                    });
                    vals.push(if bhckf171.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf171.value(row_i))
                    });
                    vals.push(if bhckf172.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf172.value(row_i))
                    });
                    vals.push(if bhckf173.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf173.value(row_i))
                    });
                    vals.push(if bhckf174.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf174.value(row_i))
                    });
                    vals.push(if bhckf175.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf175.value(row_i))
                    });
                    vals.push(if bhckf176.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf176.value(row_i))
                    });
                    vals.push(if bhckf177.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf177.value(row_i))
                    });
                    vals.push(if bhckf178.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf178.value(row_i))
                    });
                    vals.push(if bhckf179.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf179.value(row_i))
                    });
                    vals.push(if bhckf180.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf180.value(row_i))
                    });
                    vals.push(if bhckf181.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf181.value(row_i))
                    });
                    vals.push(if bhckf182.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf182.value(row_i))
                    });
                    vals.push(if bhckf183.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf183.value(row_i))
                    });
                    vals.push(if bhckf184.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf184.value(row_i))
                    });
                    vals.push(if bhckf185.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf185.value(row_i))
                    });
                    vals.push(if bhckf228.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf228.value(row_i))
                    });
                    vals.push(if bhckf229.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf229.value(row_i))
                    });
                    vals.push(if bhckf241.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf241.value(row_i))
                    });
                    vals.push(if bhckf242.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf242.value(row_i))
                    });
                    vals.push(if bhckf244.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf244.value(row_i))
                    });
                    vals.push(if bhckf245.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf245.value(row_i))
                    });
                    vals.push(if bhckf247.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf247.value(row_i))
                    });
                    vals.push(if bhckf248.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf248.value(row_i))
                    });
                    vals.push(if bhckf250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf250.value(row_i))
                    });
                    vals.push(if bhckf251.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf251.value(row_i))
                    });
                    vals.push(if bhckf253.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf253.value(row_i))
                    });
                    vals.push(if bhckf254.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf254.value(row_i))
                    });
                    vals.push(if bhckf256.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf256.value(row_i))
                    });
                    vals.push(if bhckf257.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf257.value(row_i))
                    });
                    vals.push(if bhckf259.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf259.value(row_i))
                    });
                    vals.push(if bhckf260.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf260.value(row_i))
                    });
                    vals.push(if bhckf262.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf262.value(row_i))
                    });
                    vals.push(if bhckf263.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf263.value(row_i))
                    });
                    vals.push(if bhckf264.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf264.value(row_i))
                    });
                    vals.push(if bhckf465.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf465.value(row_i))
                    });
                    vals.push(if bhckf551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf551.value(row_i))
                    });
                    vals.push(if bhckf552.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf552.value(row_i))
                    });
                    vals.push(if bhckf553.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf553.value(row_i))
                    });
                    vals.push(if bhckf554.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf554.value(row_i))
                    });
                    vals.push(if bhckf555.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf555.value(row_i))
                    });
                    vals.push(if bhckf556.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf556.value(row_i))
                    });
                    vals.push(if bhckf557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf557.value(row_i))
                    });
                    vals.push(if bhckf558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf558.value(row_i))
                    });
                    vals.push(if bhckf585.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf585.value(row_i))
                    });
                    vals.push(if bhckf586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf586.value(row_i))
                    });
                    vals.push(if bhckf587.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf587.value(row_i))
                    });
                    vals.push(if bhckf588.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf588.value(row_i))
                    });
                    vals.push(if bhckf589.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf589.value(row_i))
                    });
                    vals.push(if bhckf608.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf608.value(row_i))
                    });
                    vals.push(if bhckf639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf639.value(row_i))
                    });
                    vals.push(if bhckf640.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf640.value(row_i))
                    });
                    vals.push(if bhckf655.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf655.value(row_i))
                    });
                    vals.push(if bhckf658.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf658.value(row_i))
                    });
                    vals.push(if bhckf661.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf661.value(row_i))
                    });
                    vals.push(if bhckf662.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf662.value(row_i))
                    });
                    vals.push(if bhckf663.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf663.value(row_i))
                    });
                    vals.push(if bhckf664.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf664.value(row_i))
                    });
                    vals.push(if bhckf665.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf665.value(row_i))
                    });
                    vals.push(if bhckf666.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf666.value(row_i))
                    });
                    vals.push(if bhckf682.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf682.value(row_i))
                    });
                    vals.push(if bhckf683.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf683.value(row_i))
                    });
                    vals.push(if bhckf684.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf684.value(row_i))
                    });
                    vals.push(if bhckf685.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf685.value(row_i))
                    });
                    vals.push(if bhckf686.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf686.value(row_i))
                    });
                    vals.push(if bhckf687.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf687.value(row_i))
                    });
                    vals.push(if bhckf688.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf688.value(row_i))
                    });
                    vals.push(if bhckf689.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf689.value(row_i))
                    });
                    vals.push(if bhckf690.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf690.value(row_i))
                    });
                    vals.push(if bhckf691.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf691.value(row_i))
                    });
                    vals.push(if bhckf692.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf692.value(row_i))
                    });
                    vals.push(if bhckf693.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf693.value(row_i))
                    });
                    vals.push(if bhckf694.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf694.value(row_i))
                    });
                    vals.push(if bhckf695.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf695.value(row_i))
                    });
                    vals.push(if bhckf696.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf696.value(row_i))
                    });
                    vals.push(if bhckf697.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf697.value(row_i))
                    });
                    vals.push(if bhckf821.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf821.value(row_i))
                    });
                    vals.push(if bhckf841.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckf841.value(row_i))
                    });
                    vals.push(if bhckft28.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft28.value(row_i))
                    });
                    vals.push(if bhckft29.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft29.value(row_i))
                    });
                    vals.push(if bhckft30.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft30.value(row_i))
                    });
                    vals.push(if bhckft31.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft31.value(row_i))
                    });
                    vals.push(if bhckft32.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft32.value(row_i))
                    });
                    vals.push(if bhckft41.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft41.value(row_i))
                    });
                    vals.push(if bhckft42.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckft42.value(row_i))
                    });
                    vals.push(if bhckft43.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckft43.value(row_i))
                    });
                    vals.push(if bhckft44.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckft44.value(row_i))
                    });
                    vals.push(if bhckg091.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg091.value(row_i))
                    });
                    vals.push(if bhckg092.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg092.value(row_i))
                    });
                    vals.push(if bhckg093.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg093.value(row_i))
                    });
                    vals.push(if bhckg094.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg094.value(row_i))
                    });
                    vals.push(if bhckg095.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg095.value(row_i))
                    });
                    vals.push(if bhckg096.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg096.value(row_i))
                    });
                    vals.push(if bhckg097.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg097.value(row_i))
                    });
                    vals.push(if bhckg098.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg098.value(row_i))
                    });
                    vals.push(if bhckg099.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg099.value(row_i))
                    });
                    vals.push(if bhckg100.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg100.value(row_i))
                    });
                    vals.push(if bhckg101.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg101.value(row_i))
                    });
                    vals.push(if bhckg102.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg102.value(row_i))
                    });
                    vals.push(if bhckg103.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg103.value(row_i))
                    });
                    vals.push(if bhckg104.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg104.value(row_i))
                    });
                    vals.push(if bhckg209.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg209.value(row_i))
                    });
                    vals.push(if bhckg210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg210.value(row_i))
                    });
                    vals.push(if bhckg211.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg211.value(row_i))
                    });
                    vals.push(if bhckg212.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg212.value(row_i))
                    });
                    vals.push(if bhckg213.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg213.value(row_i))
                    });
                    vals.push(if bhckg218.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg218.value(row_i))
                    });
                    vals.push(if bhckg221.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg221.value(row_i))
                    });
                    vals.push(if bhckg234.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg234.value(row_i))
                    });
                    vals.push(if bhckg235.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg235.value(row_i))
                    });
                    vals.push(if bhckg300.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg300.value(row_i))
                    });
                    vals.push(if bhckg301.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg301.value(row_i))
                    });
                    vals.push(if bhckg302.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg302.value(row_i))
                    });
                    vals.push(if bhckg303.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg303.value(row_i))
                    });
                    vals.push(if bhckg304.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg304.value(row_i))
                    });
                    vals.push(if bhckg305.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg305.value(row_i))
                    });
                    vals.push(if bhckg306.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg306.value(row_i))
                    });
                    vals.push(if bhckg307.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg307.value(row_i))
                    });
                    vals.push(if bhckg308.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg308.value(row_i))
                    });
                    vals.push(if bhckg309.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg309.value(row_i))
                    });
                    vals.push(if bhckg310.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg310.value(row_i))
                    });
                    vals.push(if bhckg311.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg311.value(row_i))
                    });
                    vals.push(if bhckg312.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg312.value(row_i))
                    });
                    vals.push(if bhckg313.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg313.value(row_i))
                    });
                    vals.push(if bhckg314.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg314.value(row_i))
                    });
                    vals.push(if bhckg315.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg315.value(row_i))
                    });
                    vals.push(if bhckg316.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg316.value(row_i))
                    });
                    vals.push(if bhckg317.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg317.value(row_i))
                    });
                    vals.push(if bhckg318.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg318.value(row_i))
                    });
                    vals.push(if bhckg319.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg319.value(row_i))
                    });
                    vals.push(if bhckg320.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg320.value(row_i))
                    });
                    vals.push(if bhckg321.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg321.value(row_i))
                    });
                    vals.push(if bhckg322.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg322.value(row_i))
                    });
                    vals.push(if bhckg323.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg323.value(row_i))
                    });
                    vals.push(if bhckg324.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg324.value(row_i))
                    });
                    vals.push(if bhckg325.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg325.value(row_i))
                    });
                    vals.push(if bhckg326.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg326.value(row_i))
                    });
                    vals.push(if bhckg327.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg327.value(row_i))
                    });
                    vals.push(if bhckg328.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg328.value(row_i))
                    });
                    vals.push(if bhckg329.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg329.value(row_i))
                    });
                    vals.push(if bhckg330.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg330.value(row_i))
                    });
                    vals.push(if bhckg331.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg331.value(row_i))
                    });
                    vals.push(if bhckg336.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg336.value(row_i))
                    });
                    vals.push(if bhckg337.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg337.value(row_i))
                    });
                    vals.push(if bhckg338.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg338.value(row_i))
                    });
                    vals.push(if bhckg339.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg339.value(row_i))
                    });
                    vals.push(if bhckg340.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg340.value(row_i))
                    });
                    vals.push(if bhckg341.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg341.value(row_i))
                    });
                    vals.push(if bhckg342.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg342.value(row_i))
                    });
                    vals.push(if bhckg343.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg343.value(row_i))
                    });
                    vals.push(if bhckg344.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg344.value(row_i))
                    });
                    vals.push(if bhckg345.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg345.value(row_i))
                    });
                    vals.push(if bhckg346.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg346.value(row_i))
                    });
                    vals.push(if bhckg347.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg347.value(row_i))
                    });
                    vals.push(if bhckg391.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg391.value(row_i))
                    });
                    vals.push(if bhckg392.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg392.value(row_i))
                    });
                    vals.push(if bhckg395.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg395.value(row_i))
                    });
                    vals.push(if bhckg396.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg396.value(row_i))
                    });
                    vals.push(if bhckg401.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg401.value(row_i))
                    });
                    vals.push(if bhckg402.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg402.value(row_i))
                    });
                    vals.push(if bhckg403.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg403.value(row_i))
                    });
                    vals.push(if bhckg404.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg404.value(row_i))
                    });
                    vals.push(if bhckg405.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg405.value(row_i))
                    });
                    vals.push(if bhckg406.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg406.value(row_i))
                    });
                    vals.push(if bhckg407.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg407.value(row_i))
                    });
                    vals.push(if bhckg408.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg408.value(row_i))
                    });
                    vals.push(if bhckg409.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg409.value(row_i))
                    });
                    vals.push(if bhckg410.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg410.value(row_i))
                    });
                    vals.push(if bhckg411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg411.value(row_i))
                    });
                    vals.push(if bhckg412.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg412.value(row_i))
                    });
                    vals.push(if bhckg413.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg413.value(row_i))
                    });
                    vals.push(if bhckg414.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg414.value(row_i))
                    });
                    vals.push(if bhckg415.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg415.value(row_i))
                    });
                    vals.push(if bhckg416.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg416.value(row_i))
                    });
                    vals.push(if bhckg417.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg417.value(row_i))
                    });
                    vals.push(if bhckg474.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg474.value(row_i))
                    });
                    vals.push(if bhckg475.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg475.value(row_i))
                    });
                    vals.push(if bhckg476.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg476.value(row_i))
                    });
                    vals.push(if bhckg477.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg477.value(row_i))
                    });
                    vals.push(if bhckg478.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg478.value(row_i))
                    });
                    vals.push(if bhckg479.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg479.value(row_i))
                    });
                    vals.push(if bhckg480.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg480.value(row_i))
                    });
                    vals.push(if bhckg481.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg481.value(row_i))
                    });
                    vals.push(if bhckg482.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg482.value(row_i))
                    });
                    vals.push(if bhckg483.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg483.value(row_i))
                    });
                    vals.push(if bhckg484.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg484.value(row_i))
                    });
                    vals.push(if bhckg485.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg485.value(row_i))
                    });
                    vals.push(if bhckg486.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg486.value(row_i))
                    });
                    vals.push(if bhckg487.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg487.value(row_i))
                    });
                    vals.push(if bhckg488.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg488.value(row_i))
                    });
                    vals.push(if bhckg489.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg489.value(row_i))
                    });
                    vals.push(if bhckg490.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg490.value(row_i))
                    });
                    vals.push(if bhckg491.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg491.value(row_i))
                    });
                    vals.push(if bhckg492.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg492.value(row_i))
                    });
                    vals.push(if bhckg507.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg507.value(row_i))
                    });
                    vals.push(if bhckg508.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg508.value(row_i))
                    });
                    vals.push(if bhckg509.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg509.value(row_i))
                    });
                    vals.push(if bhckg510.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg510.value(row_i))
                    });
                    vals.push(if bhckg511.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg511.value(row_i))
                    });
                    vals.push(if bhckg521.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg521.value(row_i))
                    });
                    vals.push(if bhckg522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg522.value(row_i))
                    });
                    vals.push(if bhckg523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg523.value(row_i))
                    });
                    vals.push(if bhckg524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg524.value(row_i))
                    });
                    vals.push(if bhckg525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg525.value(row_i))
                    });
                    vals.push(if bhckg536.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg536.value(row_i))
                    });
                    vals.push(if bhckg537.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg537.value(row_i))
                    });
                    vals.push(if bhckg538.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg538.value(row_i))
                    });
                    vals.push(if bhckg539.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg539.value(row_i))
                    });
                    vals.push(if bhckg540.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg540.value(row_i))
                    });
                    vals.push(if bhckg541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg541.value(row_i))
                    });
                    vals.push(if bhckg542.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg542.value(row_i))
                    });
                    vals.push(if bhckg543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg543.value(row_i))
                    });
                    vals.push(if bhckg544.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg544.value(row_i))
                    });
                    vals.push(if bhckg545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg545.value(row_i))
                    });
                    vals.push(if bhckg546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg546.value(row_i))
                    });
                    vals.push(if bhckg547.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg547.value(row_i))
                    });
                    vals.push(if bhckg548.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg548.value(row_i))
                    });
                    vals.push(if bhckg549.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg549.value(row_i))
                    });
                    vals.push(if bhckg550.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg550.value(row_i))
                    });
                    vals.push(if bhckg561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg561.value(row_i))
                    });
                    vals.push(if bhckg562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg562.value(row_i))
                    });
                    vals.push(if bhckg563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg563.value(row_i))
                    });
                    vals.push(if bhckg564.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg564.value(row_i))
                    });
                    vals.push(if bhckg565.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg565.value(row_i))
                    });
                    vals.push(if bhckg566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg566.value(row_i))
                    });
                    vals.push(if bhckg567.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg567.value(row_i))
                    });
                    vals.push(if bhckg568.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg568.value(row_i))
                    });
                    vals.push(if bhckg569.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg569.value(row_i))
                    });
                    vals.push(if bhckg570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg570.value(row_i))
                    });
                    vals.push(if bhckg571.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg571.value(row_i))
                    });
                    vals.push(if bhckg572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg572.value(row_i))
                    });
                    vals.push(if bhckg573.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg573.value(row_i))
                    });
                    vals.push(if bhckg574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg574.value(row_i))
                    });
                    vals.push(if bhckg575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg575.value(row_i))
                    });
                    vals.push(if bhckg586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg586.value(row_i))
                    });
                    vals.push(if bhckg587.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg587.value(row_i))
                    });
                    vals.push(if bhckg588.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg588.value(row_i))
                    });
                    vals.push(if bhckg589.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg589.value(row_i))
                    });
                    vals.push(if bhckg590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg590.value(row_i))
                    });
                    vals.push(if bhckg597.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg597.value(row_i))
                    });
                    vals.push(if bhckg598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg598.value(row_i))
                    });
                    vals.push(if bhckg599.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg599.value(row_i))
                    });
                    vals.push(if bhckg600.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg600.value(row_i))
                    });
                    vals.push(if bhckg601.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg601.value(row_i))
                    });
                    vals.push(if bhckg602.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg602.value(row_i))
                    });
                    vals.push(if bhckg606.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg606.value(row_i))
                    });
                    vals.push(if bhckg607.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg607.value(row_i))
                    });
                    vals.push(if bhckg608.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg608.value(row_i))
                    });
                    vals.push(if bhckg609.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg609.value(row_i))
                    });
                    vals.push(if bhckg610.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg610.value(row_i))
                    });
                    vals.push(if bhckg611.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg611.value(row_i))
                    });
                    vals.push(if bhckg618.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg618.value(row_i))
                    });
                    vals.push(if bhckg619.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg619.value(row_i))
                    });
                    vals.push(if bhckg620.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg620.value(row_i))
                    });
                    vals.push(if bhckg621.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg621.value(row_i))
                    });
                    vals.push(if bhckg622.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg622.value(row_i))
                    });
                    vals.push(if bhckg623.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg623.value(row_i))
                    });
                    vals.push(if bhckg642.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg642.value(row_i))
                    });
                    vals.push(if bhckg804.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg804.value(row_i))
                    });
                    vals.push(if bhckg805.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg805.value(row_i))
                    });
                    vals.push(if bhckg806.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg806.value(row_i))
                    });
                    vals.push(if bhckg807.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg807.value(row_i))
                    });
                    vals.push(if bhckg808.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg808.value(row_i))
                    });
                    vals.push(if bhckg809.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg809.value(row_i))
                    });
                    vals.push(if bhckg894.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg894.value(row_i))
                    });
                    vals.push(if bhckg914.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg914.value(row_i))
                    });
                    vals.push(if bhckh172.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh172.value(row_i))
                    });
                    vals.push(if bhckh173.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh173.value(row_i))
                    });
                    vals.push(if bhckh174.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh174.value(row_i))
                    });
                    vals.push(if bhckh175.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh175.value(row_i))
                    });
                    vals.push(if bhckh176.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh176.value(row_i))
                    });
                    vals.push(if bhckh177.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh177.value(row_i))
                    });
                    vals.push(if bhckh178.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh178.value(row_i))
                    });
                    vals.push(if bhckh179.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh179.value(row_i))
                    });
                    vals.push(if bhckh180.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh180.value(row_i))
                    });
                    vals.push(if bhckh181.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh181.value(row_i))
                    });
                    vals.push(if bhckh182.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh182.value(row_i))
                    });
                    vals.push(if bhckh185.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh185.value(row_i))
                    });
                    vals.push(if bhckh186.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh186.value(row_i))
                    });
                    vals.push(if bhckh187.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh187.value(row_i))
                    });
                    vals.push(if bhckh188.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh188.value(row_i))
                    });
                    vals.push(if bhckh193.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh193.value(row_i))
                    });
                    vals.push(if bhckh194.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh194.value(row_i))
                    });
                    vals.push(if bhckh195.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh195.value(row_i))
                    });
                    vals.push(if bhckh196.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh196.value(row_i))
                    });
                    vals.push(if bhckh197.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh197.value(row_i))
                    });
                    vals.push(if bhckh198.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh198.value(row_i))
                    });
                    vals.push(if bhckh199.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh199.value(row_i))
                    });
                    vals.push(if bhckh200.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh200.value(row_i))
                    });
                    vals.push(if bhckh270.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh270.value(row_i))
                    });
                    vals.push(if bhckh271.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh271.value(row_i))
                    });
                    vals.push(if bhckh272.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh272.value(row_i))
                    });
                    vals.push(if bhckh273.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh273.value(row_i))
                    });
                    vals.push(if bhckh274.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh274.value(row_i))
                    });
                    vals.push(if bhckh275.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh275.value(row_i))
                    });
                    vals.push(if bhckh276.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh276.value(row_i))
                    });
                    vals.push(if bhckh277.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh277.value(row_i))
                    });
                    vals.push(if bhckh278.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh278.value(row_i))
                    });
                    vals.push(if bhckh279.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh279.value(row_i))
                    });
                    vals.push(if bhckh280.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh280.value(row_i))
                    });
                    vals.push(if bhckh281.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh281.value(row_i))
                    });
                    vals.push(if bhckh282.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh282.value(row_i))
                    });
                    vals.push(if bhckh283.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh283.value(row_i))
                    });
                    vals.push(if bhckh284.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh284.value(row_i))
                    });
                    vals.push(if bhckh285.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh285.value(row_i))
                    });
                    vals.push(if bhckh286.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh286.value(row_i))
                    });
                    vals.push(if bhckh287.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh287.value(row_i))
                    });
                    vals.push(if bhckh288.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh288.value(row_i))
                    });
                    vals.push(if bhckh293.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh293.value(row_i))
                    });
                    vals.push(if bhckh294.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh294.value(row_i))
                    });
                    vals.push(if bhckh295.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh295.value(row_i))
                    });
                    vals.push(if bhckh296.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh296.value(row_i))
                    });
                    vals.push(if bhckh297.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh297.value(row_i))
                    });
                    vals.push(if bhckh298.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh298.value(row_i))
                    });
                    vals.push(if bhckh299.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh299.value(row_i))
                    });
                    vals.push(if bhckhj78.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj78.value(row_i))
                    });
                    vals.push(if bhckhj79.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj79.value(row_i))
                    });
                    vals.push(if bhckhj80.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj80.value(row_i))
                    });
                    vals.push(if bhckhj81.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj81.value(row_i))
                    });
                    vals.push(if bhckhj82.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj82.value(row_i))
                    });
                    vals.push(if bhckhj83.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj83.value(row_i))
                    });
                    vals.push(if bhckhj84.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj84.value(row_i))
                    });
                    vals.push(if bhckhj85.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj85.value(row_i))
                    });
                    vals.push(if bhckhj88.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj88.value(row_i))
                    });
                    vals.push(if bhckhj89.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj89.value(row_i))
                    });
                    vals.push(if bhckhj92.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj92.value(row_i))
                    });
                    vals.push(if bhckhj93.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj93.value(row_i))
                    });
                    vals.push(if bhckhj94.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj94.value(row_i))
                    });
                    vals.push(if bhckhj95.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj95.value(row_i))
                    });
                    vals.push(if bhckhk03.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk03.value(row_i))
                    });
                    vals.push(if bhckhk04.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk04.value(row_i))
                    });
                    vals.push(if bhckht58.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht58.value(row_i))
                    });
                    vals.push(if bhckht59.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht59.value(row_i))
                    });
                    vals.push(if bhckht60.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht60.value(row_i))
                    });
                    vals.push(if bhckht61.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht61.value(row_i))
                    });
                    vals.push(if bhckht62.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht62.value(row_i))
                    });
                    vals.push(if bhckht63.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht63.value(row_i))
                    });
                    vals.push(if bhckht64.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht64.value(row_i))
                    });
                    vals.push(if bhckht65.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht65.value(row_i))
                    });
                    vals.push(if bhckht69.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht69.value(row_i))
                    });
                    vals.push(if bhckht80.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht80.value(row_i))
                    });
                    vals.push(if bhckht83.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht83.value(row_i))
                    });
                    vals.push(if bhckht84.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht84.value(row_i))
                    });
                    vals.push(if bhckht85.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht85.value(row_i))
                    });
                    vals.push(if bhckht87.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht87.value(row_i))
                    });
                    vals.push(if bhckht88.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht88.value(row_i))
                    });
                    vals.push(if bhckht89.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht89.value(row_i))
                    });
                    vals.push(if bhckht91.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht91.value(row_i))
                    });
                    vals.push(if bhckht92.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht92.value(row_i))
                    });
                    vals.push(if bhckht93.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht93.value(row_i))
                    });
                    vals.push(if bhckhu09.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu09.value(row_i))
                    });
                    vals.push(if bhckhu10.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu10.value(row_i))
                    });
                    vals.push(if bhckhu11.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu11.value(row_i))
                    });
                    vals.push(if bhckhu12.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu12.value(row_i))
                    });
                    vals.push(if bhckhu13.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu13.value(row_i))
                    });
                    vals.push(if bhckhu14.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu14.value(row_i))
                    });
                    vals.push(if bhckhu15.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu15.value(row_i))
                    });
                    vals.push(if bhckhu20.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu20.value(row_i))
                    });
                    vals.push(if bhckhu21.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu21.value(row_i))
                    });
                    vals.push(if bhckhu22.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu22.value(row_i))
                    });
                    vals.push(if bhckhu23.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu23.value(row_i))
                    });
                    vals.push(if bhckj320.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj320.value(row_i))
                    });
                    vals.push(if bhckj447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj447.value(row_i))
                    });
                    vals.push(if bhckj451.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj451.value(row_i))
                    });
                    vals.push(if bhckj452.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj452.value(row_i))
                    });
                    vals.push(if bhckj453.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj453.value(row_i))
                    });
                    vals.push(if bhckj454.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj454.value(row_i))
                    });
                    vals.push(if bhckj455.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj455.value(row_i))
                    });
                    vals.push(if bhckj456.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj456.value(row_i))
                    });
                    vals.push(if bhckj461.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj461.value(row_i))
                    });
                    vals.push(if bhckj462.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj462.value(row_i))
                    });
                    vals.push(if bhckj463.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj463.value(row_i))
                    });
                    vals.push(if bhckj536.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj536.value(row_i))
                    });
                    vals.push(if bhckj537.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj537.value(row_i))
                    });
                    vals.push(if bhckj981.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj981.value(row_i))
                    });
                    vals.push(if bhckj982.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj982.value(row_i))
                    });
                    vals.push(if bhckj983.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj983.value(row_i))
                    });
                    vals.push(if bhckj984.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj984.value(row_i))
                    });
                    vals.push(if bhckj985.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj985.value(row_i))
                    });
                    vals.push(if bhckj986.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj986.value(row_i))
                    });
                    vals.push(if bhckj987.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj987.value(row_i))
                    });
                    vals.push(if bhckj988.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj988.value(row_i))
                    });
                    vals.push(if bhckj989.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj989.value(row_i))
                    });
                    vals.push(if bhckj990.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj990.value(row_i))
                    });
                    vals.push(if bhckj991.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj991.value(row_i))
                    });
                    vals.push(if bhckj992.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj992.value(row_i))
                    });
                    vals.push(if bhckj993.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj993.value(row_i))
                    });
                    vals.push(if bhckj994.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj994.value(row_i))
                    });
                    vals.push(if bhckj995.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj995.value(row_i))
                    });
                    vals.push(if bhckj996.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj996.value(row_i))
                    });
                    vals.push(if bhckj997.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj997.value(row_i))
                    });
                    vals.push(if bhckj998.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj998.value(row_i))
                    });
                    vals.push(if bhckj999.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj999.value(row_i))
                    });
                    vals.push(if bhckja21.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckja21.value(row_i))
                    });
                    vals.push(if bhckja22.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckja22.value(row_i))
                    });
                    vals.push(if bhckjf76.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf76.value(row_i))
                    });
                    vals.push(if bhckjf84.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf84.value(row_i))
                    });
                    vals.push(if bhckjf85.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf85.value(row_i))
                    });
                    vals.push(if bhckjf86.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf86.value(row_i))
                    });
                    vals.push(if bhckjf87.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf87.value(row_i))
                    });
                    vals.push(if bhckjf88.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf88.value(row_i))
                    });
                    vals.push(if bhckjf89.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf89.value(row_i))
                    });
                    vals.push(if bhckjf90.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf90.value(row_i))
                    });
                    vals.push(if bhckjf91.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf91.value(row_i))
                    });
                    vals.push(if bhckjf92.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf92.value(row_i))
                    });
                    vals.push(if bhckjf93.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf93.value(row_i))
                    });
                    vals.push(if bhckjh88.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh88.value(row_i))
                    });
                    vals.push(if bhckjh91.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh91.value(row_i))
                    });
                    vals.push(if bhckjh92.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh92.value(row_i))
                    });
                    vals.push(if bhckjh93.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh93.value(row_i))
                    });
                    vals.push(if bhckjh94.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh94.value(row_i))
                    });
                    vals.push(if bhckjh97.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh97.value(row_i))
                    });
                    vals.push(if bhckjh98.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh98.value(row_i))
                    });
                    vals.push(if bhckjh99.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh99.value(row_i))
                    });
                    vals.push(if bhckjj00.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj00.value(row_i))
                    });
                    vals.push(if bhckjj01.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj01.value(row_i))
                    });
                    vals.push(if bhckjj03.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj03.value(row_i))
                    });
                    vals.push(if bhckjj04.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj04.value(row_i))
                    });
                    vals.push(if bhckjj05.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj05.value(row_i))
                    });
                    vals.push(if bhckjj06.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj06.value(row_i))
                    });
                    vals.push(if bhckjj07.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj07.value(row_i))
                    });
                    vals.push(if bhckjj08.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj08.value(row_i))
                    });
                    vals.push(if bhckjj09.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj09.value(row_i))
                    });
                    vals.push(if bhckjj11.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj11.value(row_i))
                    });
                    vals.push(if bhckjj12.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj12.value(row_i))
                    });
                    vals.push(if bhckjj13.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj13.value(row_i))
                    });
                    vals.push(if bhckjj14.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj14.value(row_i))
                    });
                    vals.push(if bhckjj15.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj15.value(row_i))
                    });
                    vals.push(if bhckjj16.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj16.value(row_i))
                    });
                    vals.push(if bhckjj17.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj17.value(row_i))
                    });
                    vals.push(if bhckjj18.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj18.value(row_i))
                    });
                    vals.push(if bhckjj19.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj19.value(row_i))
                    });
                    vals.push(if bhckjj20.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj20.value(row_i))
                    });
                    vals.push(if bhckjj21.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj21.value(row_i))
                    });
                    vals.push(if bhckjj23.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj23.value(row_i))
                    });
                    vals.push(if bhckjj24.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj24.value(row_i))
                    });
                    vals.push(if bhckjj25.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj25.value(row_i))
                    });
                    vals.push(if bhckjj26.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj26.value(row_i))
                    });
                    vals.push(if bhckjj27.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj27.value(row_i))
                    });
                    vals.push(if bhckjj28.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj28.value(row_i))
                    });
                    vals.push(if bhckjj30.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj30.value(row_i))
                    });
                    vals.push(if bhckjj31.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj31.value(row_i))
                    });
                    vals.push(if bhckjj32.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj32.value(row_i))
                    });
                    vals.push(if bhckjj34.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj34.value(row_i))
                    });
                    vals.push(if bhckk001.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk001.value(row_i))
                    });
                    vals.push(if bhckk002.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk002.value(row_i))
                    });
                    vals.push(if bhckk003.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk003.value(row_i))
                    });
                    vals.push(if bhckk004.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk004.value(row_i))
                    });
                    vals.push(if bhckk005.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk005.value(row_i))
                    });
                    vals.push(if bhckk006.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk006.value(row_i))
                    });
                    vals.push(if bhckk007.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk007.value(row_i))
                    });
                    vals.push(if bhckk008.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk008.value(row_i))
                    });
                    vals.push(if bhckk009.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk009.value(row_i))
                    });
                    vals.push(if bhckk010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk010.value(row_i))
                    });
                    vals.push(if bhckk011.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk011.value(row_i))
                    });
                    vals.push(if bhckk012.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk012.value(row_i))
                    });
                    vals.push(if bhckk013.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk013.value(row_i))
                    });
                    vals.push(if bhckk014.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk014.value(row_i))
                    });
                    vals.push(if bhckk015.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk015.value(row_i))
                    });
                    vals.push(if bhckk016.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk016.value(row_i))
                    });
                    vals.push(if bhckk017.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk017.value(row_i))
                    });
                    vals.push(if bhckk018.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk018.value(row_i))
                    });
                    vals.push(if bhckk019.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk019.value(row_i))
                    });
                    vals.push(if bhckk020.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk020.value(row_i))
                    });
                    vals.push(if bhckk021.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk021.value(row_i))
                    });
                    vals.push(if bhckk022.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk022.value(row_i))
                    });
                    vals.push(if bhckk023.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk023.value(row_i))
                    });
                    vals.push(if bhckk024.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk024.value(row_i))
                    });
                    vals.push(if bhckk025.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk025.value(row_i))
                    });
                    vals.push(if bhckk026.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk026.value(row_i))
                    });
                    vals.push(if bhckk027.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk027.value(row_i))
                    });
                    vals.push(if bhckk028.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk028.value(row_i))
                    });
                    vals.push(if bhckk029.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk029.value(row_i))
                    });
                    vals.push(if bhckk030.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk030.value(row_i))
                    });
                    vals.push(if bhckk031.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk031.value(row_i))
                    });
                    vals.push(if bhckk032.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk032.value(row_i))
                    });
                    vals.push(if bhckk033.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk033.value(row_i))
                    });
                    vals.push(if bhckk034.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk034.value(row_i))
                    });
                    vals.push(if bhckk035.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk035.value(row_i))
                    });
                    vals.push(if bhckk036.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk036.value(row_i))
                    });
                    vals.push(if bhckk037.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk037.value(row_i))
                    });
                    vals.push(if bhckk038.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk038.value(row_i))
                    });
                    vals.push(if bhckk039.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk039.value(row_i))
                    });
                    vals.push(if bhckk040.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk040.value(row_i))
                    });
                    vals.push(if bhckk041.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk041.value(row_i))
                    });
                    vals.push(if bhckk072.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk072.value(row_i))
                    });
                    vals.push(if bhckk073.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk073.value(row_i))
                    });
                    vals.push(if bhckk074.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk074.value(row_i))
                    });
                    vals.push(if bhckk075.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk075.value(row_i))
                    });
                    vals.push(if bhckk076.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk076.value(row_i))
                    });
                    vals.push(if bhckk077.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk077.value(row_i))
                    });
                    vals.push(if bhckk078.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk078.value(row_i))
                    });
                    vals.push(if bhckk079.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk079.value(row_i))
                    });
                    vals.push(if bhckk080.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk080.value(row_i))
                    });
                    vals.push(if bhckk081.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk081.value(row_i))
                    });
                    vals.push(if bhckk082.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk082.value(row_i))
                    });
                    vals.push(if bhckk083.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk083.value(row_i))
                    });
                    vals.push(if bhckk084.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk084.value(row_i))
                    });
                    vals.push(if bhckk085.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk085.value(row_i))
                    });
                    vals.push(if bhckk086.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk086.value(row_i))
                    });
                    vals.push(if bhckk087.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk087.value(row_i))
                    });
                    vals.push(if bhckk088.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk088.value(row_i))
                    });
                    vals.push(if bhckk089.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk089.value(row_i))
                    });
                    vals.push(if bhckk090.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk090.value(row_i))
                    });
                    vals.push(if bhckk091.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk091.value(row_i))
                    });
                    vals.push(if bhckk092.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk092.value(row_i))
                    });
                    vals.push(if bhckk093.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk093.value(row_i))
                    });
                    vals.push(if bhckk094.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk094.value(row_i))
                    });
                    vals.push(if bhckk095.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk095.value(row_i))
                    });
                    vals.push(if bhckk096.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk096.value(row_i))
                    });
                    vals.push(if bhckk097.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk097.value(row_i))
                    });
                    vals.push(if bhckk098.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk098.value(row_i))
                    });
                    vals.push(if bhckk099.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk099.value(row_i))
                    });
                    vals.push(if bhckk100.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk100.value(row_i))
                    });
                    vals.push(if bhckk101.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk101.value(row_i))
                    });
                    vals.push(if bhckk120.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk120.value(row_i))
                    });
                    vals.push(if bhckk121.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk121.value(row_i))
                    });
                    vals.push(if bhckk122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk122.value(row_i))
                    });
                    vals.push(if bhckk123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk123.value(row_i))
                    });
                    vals.push(if bhckk124.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk124.value(row_i))
                    });
                    vals.push(if bhckk125.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk125.value(row_i))
                    });
                    vals.push(if bhckk126.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk126.value(row_i))
                    });
                    vals.push(if bhckk127.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk127.value(row_i))
                    });
                    vals.push(if bhckk128.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk128.value(row_i))
                    });
                    vals.push(if bhckk129.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk129.value(row_i))
                    });
                    vals.push(if bhckk134.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk134.value(row_i))
                    });
                    vals.push(if bhckk135.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk135.value(row_i))
                    });
                    vals.push(if bhckk136.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk136.value(row_i))
                    });
                    vals.push(if bhckk137.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk137.value(row_i))
                    });
                    vals.push(if bhckk138.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk138.value(row_i))
                    });
                    vals.push(if bhckk139.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk139.value(row_i))
                    });
                    vals.push(if bhckk140.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk140.value(row_i))
                    });
                    vals.push(if bhckk142.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk142.value(row_i))
                    });
                    vals.push(if bhckk143.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk143.value(row_i))
                    });
                    vals.push(if bhckk144.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk144.value(row_i))
                    });
                    vals.push(if bhckk145.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk145.value(row_i))
                    });
                    vals.push(if bhckk146.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk146.value(row_i))
                    });
                    vals.push(if bhckk147.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk147.value(row_i))
                    });
                    vals.push(if bhckk148.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk148.value(row_i))
                    });
                    vals.push(if bhckk149.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk149.value(row_i))
                    });
                    vals.push(if bhckk150.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk150.value(row_i))
                    });
                    vals.push(if bhckk151.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk151.value(row_i))
                    });
                    vals.push(if bhckk152.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk152.value(row_i))
                    });
                    vals.push(if bhckk153.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk153.value(row_i))
                    });
                    vals.push(if bhckk154.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk154.value(row_i))
                    });
                    vals.push(if bhckk155.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk155.value(row_i))
                    });
                    vals.push(if bhckk156.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk156.value(row_i))
                    });
                    vals.push(if bhckk157.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk157.value(row_i))
                    });
                    vals.push(if bhckk163.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk163.value(row_i))
                    });
                    vals.push(if bhckk164.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk164.value(row_i))
                    });
                    vals.push(if bhckk165.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk165.value(row_i))
                    });
                    vals.push(if bhckk167.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk167.value(row_i))
                    });
                    vals.push(if bhckk168.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk168.value(row_i))
                    });
                    vals.push(if bhckk178.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk178.value(row_i))
                    });
                    vals.push(if bhckk179.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk179.value(row_i))
                    });
                    vals.push(if bhckk180.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk180.value(row_i))
                    });
                    vals.push(if bhckk181.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk181.value(row_i))
                    });
                    vals.push(if bhckk182.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk182.value(row_i))
                    });
                    vals.push(if bhckk183.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk183.value(row_i))
                    });
                    vals.push(if bhckk184.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk184.value(row_i))
                    });
                    vals.push(if bhckk185.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk185.value(row_i))
                    });
                    vals.push(if bhckk186.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk186.value(row_i))
                    });
                    vals.push(if bhckk192.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk192.value(row_i))
                    });
                    vals.push(if bhckk193.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk193.value(row_i))
                    });
                    vals.push(if bhckk194.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk194.value(row_i))
                    });
                    vals.push(if bhckk196.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk196.value(row_i))
                    });
                    vals.push(if bhckk201.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk201.value(row_i))
                    });
                    vals.push(if bhckk202.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk202.value(row_i))
                    });
                    vals.push(if bhckk203.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk203.value(row_i))
                    });
                    vals.push(if bhckk204.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk204.value(row_i))
                    });
                    vals.push(if bhckk205.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk205.value(row_i))
                    });
                    vals.push(if bhckk207.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk207.value(row_i))
                    });
                    vals.push(if bhckk208.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk208.value(row_i))
                    });
                    vals.push(if bhckk212.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk212.value(row_i))
                    });
                    vals.push(if bhckk213.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk213.value(row_i))
                    });
                    vals.push(if bhckk214.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk214.value(row_i))
                    });
                    vals.push(if bhckk215.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk215.value(row_i))
                    });
                    vals.push(if bhckk216.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk216.value(row_i))
                    });
                    vals.push(if bhckk217.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk217.value(row_i))
                    });
                    vals.push(if bhckk218.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk218.value(row_i))
                    });
                    vals.push(if bhckk267.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk267.value(row_i))
                    });
                    vals.push(if bhckk269.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk269.value(row_i))
                    });
                    vals.push(if bhckk270.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk270.value(row_i))
                    });
                    vals.push(if bhckk271.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk271.value(row_i))
                    });
                    vals.push(if bhckk272.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk272.value(row_i))
                    });
                    vals.push(if bhckk273.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk273.value(row_i))
                    });
                    vals.push(if bhckk274.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk274.value(row_i))
                    });
                    vals.push(if bhckk275.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk275.value(row_i))
                    });
                    vals.push(if bhckk276.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk276.value(row_i))
                    });
                    vals.push(if bhckk277.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk277.value(row_i))
                    });
                    vals.push(if bhckk278.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk278.value(row_i))
                    });
                    vals.push(if bhckk279.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk279.value(row_i))
                    });
                    vals.push(if bhckk280.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk280.value(row_i))
                    });
                    vals.push(if bhckk281.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk281.value(row_i))
                    });
                    vals.push(if bhckk282.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk282.value(row_i))
                    });
                    vals.push(if bhckk283.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk283.value(row_i))
                    });
                    vals.push(if bhckk284.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk284.value(row_i))
                    });
                    vals.push(if bhckk285.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk285.value(row_i))
                    });
                    vals.push(if bhckk286.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk286.value(row_i))
                    });
                    vals.push(if bhckk287.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk287.value(row_i))
                    });
                    vals.push(if bhckk288.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk288.value(row_i))
                    });
                    vals.push(if bhckkx46.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx46.value(row_i))
                    });
                    vals.push(if bhckkx47.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx47.value(row_i))
                    });
                    vals.push(if bhckkx50.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx50.value(row_i))
                    });
                    vals.push(if bhckkx51.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx51.value(row_i))
                    });
                    vals.push(if bhckkx52.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx52.value(row_i))
                    });
                    vals.push(if bhckkx53.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx53.value(row_i))
                    });
                    vals.push(if bhckkx54.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx54.value(row_i))
                    });
                    vals.push(if bhckkx55.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx55.value(row_i))
                    });
                    vals.push(if bhckkx57.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx57.value(row_i))
                    });
                    vals.push(if bhckkx58.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx58.value(row_i))
                    });
                    vals.push(if bhckkx60.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx60.value(row_i))
                    });
                    vals.push(if bhckkx61.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx61.value(row_i))
                    });
                    vals.push(if bhckkx62.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx62.value(row_i))
                    });
                    vals.push(if bhckkx63.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx63.value(row_i))
                    });
                    vals.push(if bhckkx64.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx64.value(row_i))
                    });
                    vals.push(if bhckkx65.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx65.value(row_i))
                    });
                    vals.push(if bhckky38.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckky38.value(row_i))
                    });
                    vals.push(if bhcklg24.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcklg24.value(row_i))
                    });
                    vals.push(if bhcklg26.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcklg26.value(row_i))
                    });
                    vals.push(if bhckm727.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm727.value(row_i))
                    });
                    vals.push(if bhckm728.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm728.value(row_i))
                    });
                    vals.push(if bhckm729.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm729.value(row_i))
                    });
                    vals.push(if bhckm730.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm730.value(row_i))
                    });
                    vals.push(if bhckm731.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm731.value(row_i))
                    });
                    vals.push(if bhckm732.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm732.value(row_i))
                    });
                    vals.push(if bhckm733.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm733.value(row_i))
                    });
                    vals.push(if bhckm734.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm734.value(row_i))
                    });
                    vals.push(if bhckm735.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm735.value(row_i))
                    });
                    vals.push(if bhckm736.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm736.value(row_i))
                    });
                    vals.push(if bhckm737.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm737.value(row_i))
                    });
                    vals.push(if bhckm738.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm738.value(row_i))
                    });
                    vals.push(if bhckm739.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm739.value(row_i))
                    });
                    vals.push(if bhckm740.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm740.value(row_i))
                    });
                    vals.push(if bhckm741.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm741.value(row_i))
                    });
                    vals.push(if bhckm742.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm742.value(row_i))
                    });
                    vals.push(if bhckm743.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm743.value(row_i))
                    });
                    vals.push(if bhckm744.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm744.value(row_i))
                    });
                    vals.push(if bhckm962.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm962.value(row_i))
                    });
                    vals.push(if bhckmg94.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckmg94.value(row_i))
                    });
                    vals.push(if bhcks396.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks396.value(row_i))
                    });
                    vals.push(if bhcks397.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks397.value(row_i))
                    });
                    vals.push(if bhcks398.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks398.value(row_i))
                    });
                    vals.push(if bhcks399.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks399.value(row_i))
                    });
                    vals.push(if bhcks400.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks400.value(row_i))
                    });
                    vals.push(if bhcks402.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks402.value(row_i))
                    });
                    vals.push(if bhcks403.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks403.value(row_i))
                    });
                    vals.push(if bhcks405.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks405.value(row_i))
                    });
                    vals.push(if bhcks406.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks406.value(row_i))
                    });
                    vals.push(if bhcks410.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks410.value(row_i))
                    });
                    vals.push(if bhcks411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks411.value(row_i))
                    });
                    vals.push(if bhcks414.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks414.value(row_i))
                    });
                    vals.push(if bhcks415.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks415.value(row_i))
                    });
                    vals.push(if bhcks416.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks416.value(row_i))
                    });
                    vals.push(if bhcks417.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks417.value(row_i))
                    });
                    vals.push(if bhcks420.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks420.value(row_i))
                    });
                    vals.push(if bhcks421.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks421.value(row_i))
                    });
                    vals.push(if bhcks424.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks424.value(row_i))
                    });
                    vals.push(if bhcks425.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks425.value(row_i))
                    });
                    vals.push(if bhcks426.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks426.value(row_i))
                    });
                    vals.push(if bhcks427.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks427.value(row_i))
                    });
                    vals.push(if bhcks428.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks428.value(row_i))
                    });
                    vals.push(if bhcks429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks429.value(row_i))
                    });
                    vals.push(if bhcks432.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks432.value(row_i))
                    });
                    vals.push(if bhcks433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks433.value(row_i))
                    });
                    vals.push(if bhcks434.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks434.value(row_i))
                    });
                    vals.push(if bhcks435.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks435.value(row_i))
                    });
                    vals.push(if bhcks436.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks436.value(row_i))
                    });
                    vals.push(if bhcks437.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks437.value(row_i))
                    });
                    vals.push(if bhcks440.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks440.value(row_i))
                    });
                    vals.push(if bhcks441.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks441.value(row_i))
                    });
                    vals.push(if bhcks442.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks442.value(row_i))
                    });
                    vals.push(if bhcks443.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks443.value(row_i))
                    });
                    vals.push(if bhcks446.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks446.value(row_i))
                    });
                    vals.push(if bhcks447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks447.value(row_i))
                    });
                    vals.push(if bhcks450.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks450.value(row_i))
                    });
                    vals.push(if bhcks451.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks451.value(row_i))
                    });
                    vals.push(if bhcks452.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks452.value(row_i))
                    });
                    vals.push(if bhcks453.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks453.value(row_i))
                    });
                    vals.push(if bhcks454.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks454.value(row_i))
                    });
                    vals.push(if bhcks455.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks455.value(row_i))
                    });
                    vals.push(if bhcks458.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks458.value(row_i))
                    });
                    vals.push(if bhcks459.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks459.value(row_i))
                    });
                    vals.push(if bhcks460.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks460.value(row_i))
                    });
                    vals.push(if bhcks461.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks461.value(row_i))
                    });
                    vals.push(if bhcks462.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks462.value(row_i))
                    });
                    vals.push(if bhcks463.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks463.value(row_i))
                    });
                    vals.push(if bhcks469.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks469.value(row_i))
                    });
                    vals.push(if bhcks470.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks470.value(row_i))
                    });
                    vals.push(if bhcks471.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks471.value(row_i))
                    });
                    vals.push(if bhcks476.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks476.value(row_i))
                    });
                    vals.push(if bhcks477.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks477.value(row_i))
                    });
                    vals.push(if bhcks478.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks478.value(row_i))
                    });
                    vals.push(if bhcks479.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks479.value(row_i))
                    });
                    vals.push(if bhcks481.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks481.value(row_i))
                    });
                    vals.push(if bhcks482.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks482.value(row_i))
                    });
                    vals.push(if bhcks483.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks483.value(row_i))
                    });
                    vals.push(if bhcks484.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks484.value(row_i))
                    });
                    vals.push(if bhcks486.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks486.value(row_i))
                    });
                    vals.push(if bhcks487.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks487.value(row_i))
                    });
                    vals.push(if bhcks488.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks488.value(row_i))
                    });
                    vals.push(if bhcks489.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks489.value(row_i))
                    });
                    vals.push(if bhcks491.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks491.value(row_i))
                    });
                    vals.push(if bhcks492.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks492.value(row_i))
                    });
                    vals.push(if bhcks493.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks493.value(row_i))
                    });
                    vals.push(if bhcks494.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks494.value(row_i))
                    });
                    vals.push(if bhcks496.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks496.value(row_i))
                    });
                    vals.push(if bhcks497.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks497.value(row_i))
                    });
                    vals.push(if bhcks498.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks498.value(row_i))
                    });
                    vals.push(if bhcks499.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks499.value(row_i))
                    });
                    vals.push(if bhcks511.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks511.value(row_i))
                    });
                    vals.push(if bhcks513.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks513.value(row_i))
                    });
                    vals.push(if bhcks524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks524.value(row_i))
                    });
                    vals.push(if bhcks549.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks549.value(row_i))
                    });
                    vals.push(if bhcks550.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks550.value(row_i))
                    });
                    vals.push(if bhcks551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks551.value(row_i))
                    });
                    vals.push(if bhcks552.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks552.value(row_i))
                    });
                    vals.push(if bhcks554.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks554.value(row_i))
                    });
                    vals.push(if bhcks555.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks555.value(row_i))
                    });
                    vals.push(if bhcks556.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks556.value(row_i))
                    });
                    vals.push(if bhcks557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks557.value(row_i))
                    });
                    vals.push(if bhcks582.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks582.value(row_i))
                    });
                    vals.push(if bhcks583.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks583.value(row_i))
                    });
                    vals.push(if bhcks584.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks584.value(row_i))
                    });
                    vals.push(if bhcks585.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks585.value(row_i))
                    });
                    vals.push(if bhcks586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks586.value(row_i))
                    });
                    vals.push(if bhcks587.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks587.value(row_i))
                    });
                    vals.push(if bhcks588.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks588.value(row_i))
                    });
                    vals.push(if bhcks589.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks589.value(row_i))
                    });
                    vals.push(if bhcks590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks590.value(row_i))
                    });
                    vals.push(if bhcks591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks591.value(row_i))
                    });
                    vals.push(if bhcks592.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks592.value(row_i))
                    });
                    vals.push(if bhcks593.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks593.value(row_i))
                    });
                    vals.push(if bhcks594.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks594.value(row_i))
                    });
                    vals.push(if bhcks595.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks595.value(row_i))
                    });
                    vals.push(if bhcks596.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks596.value(row_i))
                    });
                    vals.push(if bhcks597.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks597.value(row_i))
                    });
                    vals.push(if bhcks598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks598.value(row_i))
                    });
                    vals.push(if bhcks599.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks599.value(row_i))
                    });
                    vals.push(if bhcks600.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks600.value(row_i))
                    });
                    vals.push(if bhcks601.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks601.value(row_i))
                    });
                    vals.push(if bhcks602.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks602.value(row_i))
                    });
                    vals.push(if bhcks603.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks603.value(row_i))
                    });
                    vals.push(if bhcks604.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks604.value(row_i))
                    });
                    vals.push(if bhcks605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks605.value(row_i))
                    });
                    vals.push(if bhcks606.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks606.value(row_i))
                    });
                    vals.push(if bhcks607.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks607.value(row_i))
                    });
                    vals.push(if bhcks608.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks608.value(row_i))
                    });
                    vals.push(if bhcks609.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks609.value(row_i))
                    });
                    vals.push(if bhcks610.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks610.value(row_i))
                    });
                    vals.push(if bhcks611.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks611.value(row_i))
                    });
                    vals.push(if bhcks612.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks612.value(row_i))
                    });
                    vals.push(if bhcks613.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks613.value(row_i))
                    });
                    vals.push(if bhcks614.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks614.value(row_i))
                    });
                    vals.push(if bhcks615.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks615.value(row_i))
                    });
                    vals.push(if bhcks616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks616.value(row_i))
                    });
                    vals.push(if bhcks617.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks617.value(row_i))
                    });
                    vals.push(if bhcks618.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks618.value(row_i))
                    });
                    vals.push(if bhcks619.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks619.value(row_i))
                    });
                    vals.push(if bhcks620.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks620.value(row_i))
                    });
                    vals.push(if bhcks621.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks621.value(row_i))
                    });
                    vals.push(if bhcks622.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks622.value(row_i))
                    });
                    vals.push(if bhcks623.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks623.value(row_i))
                    });
                    vals.push(if bhckt047.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckt047.value(row_i))
                    });
                    vals.push(if bhcky923.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcky923.value(row_i))
                    });
                    vals.push(if bhcky924.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcky924.value(row_i))
                    });
                    vals.push(if rssd9001.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9001.value(row_i))
                    });
                    vals.push(if rssd9017.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9017.value(row_i).into())
                    });
                    vals.push(if rssd9999.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9999.value(row_i))
                    });
                    vals.push(if wrdsdownloaddate.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Date(wrdsdownloaddate.value(row_i))
                    });
                    out.push(Row::new(vals));
                }
            }

            Ok::<Vec<Row>, AppError>(out)
        })
        .await?
    }
}
