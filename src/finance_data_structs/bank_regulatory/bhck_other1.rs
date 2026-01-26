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
pub struct BhckOther1 {
    pub bhbc3368: Option<f64>,
    pub bhbc3402: Option<f64>,
    pub bhbc3516: Option<f64>,
    pub bhbc3519: Option<f64>,
    pub bhbc4070: Option<f64>,
    pub bhbc4073: Option<f64>,
    pub bhbc4074: Option<f64>,
    pub bhbc4079: Option<f64>,
    pub bhbc4091: Option<f64>,
    pub bhbc4093: Option<f64>,
    pub bhbc4094: Option<f64>,
    pub bhbc4107: Option<f64>,
    pub bhbc4135: Option<f64>,
    pub bhbc4218: Option<f64>,
    pub bhbc4230: Option<f64>,
    pub bhbc4301: Option<f64>,
    pub bhbc4302: Option<f64>,
    pub bhbc4320: Option<f64>,
    pub bhbc4340: Option<f64>,
    pub bhbc4421: Option<f64>,
    pub bhbc4475: Option<f64>,
    pub bhbc4484: Option<f64>,
    pub bhbc4519: Option<f64>,
    pub bhbc6061: Option<f64>,
    pub bhbca220: Option<f64>,
    pub bhbcb490: Option<f64>,
    pub bhbcb491: Option<f64>,
    pub bhbcb493: Option<f64>,
    pub bhbcb494: Option<f64>,
    pub bhbcc216: Option<f64>,
    pub bhbcjj33: Option<f64>,
    pub bhc00010: Option<f64>,
    pub bhc00390: Option<f64>,
    pub bhc01350: Option<f64>,
    pub bhc01754: Option<f64>,
    pub bhc01773: Option<f64>,
    pub bhc02122: Option<f64>,
    pub bhc02170: Option<f64>,
    pub bhc03411: Option<f64>,
    pub bhc03429: Option<f64>,
    pub bhc03433: Option<f64>,
    pub bhc03545: Option<f64>,
    pub bhc05369: Option<f64>,
    pub bhc06551: Option<f64>,
    pub bhc06563: Option<f64>,
    pub bhc06566: Option<f64>,
    pub bhc06570: Option<f64>,
    pub bhc06572: Option<f64>,
    pub bhc06574: Option<f64>,
    pub bhc06575: Option<f64>,
    pub bhc06598: Option<f64>,
    pub bhc06601: Option<bool>,
    pub bhc06602: Option<f64>,
    pub bhc06603: Option<f64>,
    pub bhc0a167: Option<f64>,
    pub bhc0a250: Option<f64>,
    pub bhc0b528: Option<f64>,
    pub bhc0b546: Option<f64>,
    pub bhc0b639: Option<f64>,
    pub bhc0b675: Option<f64>,
    pub bhc0b681: Option<f64>,
    pub bhc0c225: Option<f64>,
    pub bhc0g591: Option<f64>,
    pub bhc20010: Option<f64>,
    pub bhc20390: Option<f64>,
    pub bhc21350: Option<f64>,
    pub bhc21754: Option<f64>,
    pub bhc21773: Option<f64>,
    pub bhc22122: Option<f64>,
    pub bhc22170: Option<f64>,
    pub bhc23411: Option<f64>,
    pub bhc23429: Option<f64>,
    pub bhc23433: Option<f64>,
    pub bhc23545: Option<f64>,
    pub bhc25369: Option<f64>,
    pub bhc26551: Option<f64>,
    pub bhc26563: Option<f64>,
    pub bhc26566: Option<f64>,
    pub bhc26570: Option<f64>,
    pub bhc26572: Option<f64>,
    pub bhc26574: Option<f64>,
    pub bhc26575: Option<f64>,
    pub bhc26598: Option<f64>,
    pub bhc26601: Option<f64>,
    pub bhc26602: Option<f64>,
    pub bhc26603: Option<f64>,
    pub bhc2a167: Option<f64>,
    pub bhc2a250: Option<f64>,
    pub bhc2b528: Option<f64>,
    pub bhc2b546: Option<f64>,
    pub bhc2b639: Option<f64>,
    pub bhc2b675: Option<f64>,
    pub bhc2b681: Option<f64>,
    pub bhc2c225: Option<f64>,
    pub bhc2g591: Option<f64>,
    pub bhc50390: Option<f64>,
    pub bhc51350: Option<bool>,
    pub bhc51754: Option<f64>,
    pub bhc51773: Option<f64>,
    pub bhc52122: Option<f64>,
    pub bhc52170: Option<f64>,
    pub bhc53411: Option<f64>,
    pub bhc53433: Option<f64>,
    pub bhc53545: Option<f64>,
    pub bhc55369: Option<f64>,
    pub bhc56551: Option<f64>,
    pub bhc56563: Option<f64>,
    pub bhc56566: Option<f64>,
    pub bhc56570: Option<f64>,
    pub bhc56572: Option<f64>,
    pub bhc56574: Option<f64>,
    pub bhc56575: Option<f64>,
    pub bhc56598: Option<f64>,
    pub bhc56602: Option<f64>,
    pub bhc56603: Option<f64>,
    pub bhc5a167: Option<f64>,
    pub bhc5a250: Option<f64>,
    pub bhc5b528: Option<f64>,
    pub bhc5b546: Option<f64>,
    pub bhc5b639: Option<f64>,
    pub bhc5b675: Option<f64>,
    pub bhc5b681: Option<f64>,
    pub bhc5g591: Option<f64>,
    pub bhc90010: Option<f64>,
    pub bhc90390: Option<f64>,
    pub bhc91350: Option<f64>,
    pub bhc91727: Option<f64>,
    pub bhc91754: Option<f64>,
    pub bhc91773: Option<f64>,
    pub bhc92122: Option<f64>,
    pub bhc92170: Option<f64>,
    pub bhc93411: Option<f64>,
    pub bhc93429: Option<f64>,
    pub bhc93433: Option<f64>,
    pub bhc93545: Option<f64>,
    pub bhc95369: Option<f64>,
    pub bhc96551: Option<f64>,
    pub bhc96563: Option<f64>,
    pub bhc96566: Option<f64>,
    pub bhc96570: Option<f64>,
    pub bhc96572: Option<f64>,
    pub bhc96574: Option<f64>,
    pub bhc96575: Option<f64>,
    pub bhc96598: Option<f64>,
    pub bhc96602: Option<f64>,
    pub bhc96603: Option<f64>,
    pub bhc9a250: Option<f64>,
    pub bhc9b528: Option<f64>,
    pub bhc9b541: Option<f64>,
    pub bhc9b546: Option<f64>,
    pub bhc9b639: Option<f64>,
    pub bhc9b675: Option<f64>,
    pub bhc9b681: Option<f64>,
    pub bhc9c225: Option<f64>,
    pub bhc9g591: Option<f64>,
    pub bhca2170: Option<f64>,
    pub bhca3792: Option<f64>,
    pub bhca5310: Option<f64>,
    pub bhca5311: Option<f64>,
    pub bhca7204: Option<f64>,
    pub bhca7205: Option<f64>,
    pub bhca7206: Option<f64>,
    pub bhca8274: Option<f64>,
    pub bhcaa223: Option<f64>,
    pub bhcaa224: Option<f64>,
    pub bhcab530: Option<f64>,
    pub bhcab596: Option<f64>,
    pub bhcah036: Option<f64>,
    pub bhcah311: Option<f64>,
    pub bhcah312: Option<bool>,
    pub bhcah313: Option<f64>,
    pub bhcah314: Option<f64>,
    pub bhcajj29: Option<f64>,
    pub bhcakw00: Option<f64>,
    pub bhcakw03: Option<f64>,
    pub bhcakx77: Option<f64>,
    pub bhcakx78: Option<f64>,
    pub bhcakx79: Option<f64>,
    pub bhcakx80: Option<f64>,
    pub bhcakx81: Option<f64>,
    pub bhcakx82: Option<f64>,
    pub bhcakx83: Option<f64>,
    pub bhcalb58: Option<f64>,
    pub bhcalb59: Option<f64>,
    pub bhcalb60: Option<f64>,
    pub bhcalb61: Option<f64>,
    pub bhcale74: Option<f64>,
    pub bhcale85: Option<f64>,
    pub bhcale86: Option<f64>,
    pub bhcale87: Option<f64>,
    pub bhcale88: Option<f64>,
    pub bhcale89: Option<f64>,
    pub bhcale90: Option<f64>,
    pub bhcale91: Option<f64>,
    pub bhcale92: Option<f64>,
    pub bhcalf21: Option<f64>,
    pub bhcalf22: Option<f64>,
    pub bhcalf23: Option<f64>,
    pub bhcalf24: Option<f64>,
    pub bhcalf25: Option<f64>,
    pub bhcalf27: Option<f64>,
    pub bhcalf28: Option<f64>,
    pub bhcamk66: Option<f64>,
    pub bhcamk76: Option<f64>,
    pub bhcamk77: Option<f64>,
    pub bhcamk78: Option<f64>,
    pub bhcanc99: Option<f64>,
    pub bhcap742: Option<f64>,
    pub bhcap793: Option<f64>,
    pub bhcap838: Option<f64>,
    pub bhcap839: Option<f64>,
    pub bhcap840: Option<f64>,
    pub bhcap841: Option<f64>,
    pub bhcap842: Option<f64>,
    pub bhcap843: Option<f64>,
    pub bhcap844: Option<f64>,
    pub bhcap845: Option<f64>,
    pub bhcap846: Option<f64>,
    pub bhcap847: Option<f64>,
    pub bhcap848: Option<f64>,
    pub bhcap849: Option<f64>,
    pub bhcap850: Option<f64>,
    pub bhcap851: Option<f64>,
    pub bhcap852: Option<f64>,
    pub bhcap853: Option<f64>,
    pub bhcap854: Option<f64>,
    pub bhcap855: Option<f64>,
    pub bhcap856: Option<f64>,
    pub bhcap857: Option<f64>,
    pub bhcap858: Option<f64>,
    pub bhcap859: Option<f64>,
    pub bhcap860: Option<f64>,
    pub bhcap861: Option<f64>,
    pub bhcap862: Option<f64>,
    pub bhcap863: Option<f64>,
    pub bhcap864: Option<f64>,
    pub bhcap865: Option<f64>,
    pub bhcap866: Option<f64>,
    pub bhcap867: Option<f64>,
    pub bhcap868: Option<f64>,
    pub bhcap870: Option<f64>,
    pub bhcap872: Option<f64>,
    pub bhcap875: Option<f64>,
    pub bhcaq257: Option<f64>,
    pub bhcaq258: Option<f64>,
    pub bhcas540: Option<f64>,
    pub bhcb2210: Option<f64>,
    pub bhcb2389: Option<f64>,
    pub bhcb2604: Option<f64>,
    pub bhcb3187: Option<f64>,
    pub bhcb6648: Option<f64>,
    pub bhcbhk29: Option<f64>,
    pub bhcbj474: Option<f64>,
    pub bhce0010: Option<f64>,
    pub bhce1727: Option<f64>,
    pub bhce1754: Option<f64>,
    pub bhce1773: Option<f64>,
    pub bhce2170: Option<f64>,
    pub bhce3123: Option<f64>,
    pub bhce3411: Option<f64>,
    pub bhce3429: Option<f64>,
    pub bhce3433: Option<f64>,
    pub bhce3545: Option<f64>,
    pub bhce5369: Option<f64>,
    pub bhce6566: Option<f64>,
    pub bhce6570: Option<f64>,
    pub bhce6572: Option<f64>,
    pub bhcea167: Option<f64>,
    pub bhcea250: Option<f64>,
    pub bhceb528: Option<f64>,
    pub bhceb541: Option<f64>,
    pub bhceb546: Option<f64>,
    pub bhceb639: Option<f64>,
    pub bhceb675: Option<f64>,
    pub bhceb681: Option<f64>,
    pub bhceg591: Option<f64>,
    pub bhcm3531: Option<f64>,
    pub bhcm3532: Option<f64>,
    pub bhcm3533: Option<f64>,
    pub bhcm3534: Option<f64>,
    pub bhcm3535: Option<f64>,
    pub bhcm3536: Option<f64>,
    pub bhcm3537: Option<f64>,
    pub bhcm3541: Option<f64>,
    pub bhcm3543: Option<f64>,
    pub bhcp0010: Option<f64>,
    pub bhcp0087: Option<f64>,
    pub bhcp0201: Option<f64>,
    pub bhcp0202: Option<f64>,
    pub bhcp0203: Option<f64>,
    pub bhcp0204: Option<f64>,
    pub bhcp0205: Option<f64>,
    pub bhcp0206: Option<f64>,
    pub bhcp0207: Option<f64>,
    pub bhcp0208: Option<f64>,
    pub bhcp0209: Option<f64>,
    pub bhcp0210: Option<f64>,
    pub bhcp0277: Option<f64>,
    pub bhcp0279: Option<f64>,
    pub bhcp0362: Option<f64>,
    pub bhcp0363: Option<f64>,
    pub bhcp0364: Option<f64>,
    pub bhcp0365: Option<f64>,
    pub bhcp0368: Option<f64>,
    pub bhcp0400: Option<f64>,
    pub bhcp0416: Option<f64>,
    pub bhcp0447: Option<f64>,
    pub bhcp0467: Option<f64>,
    pub bhcp0496: Option<f64>,
    pub bhcp0508: Option<f64>,
    pub bhcp0512: Option<f64>,
    pub bhcp0515: Option<f64>,
    pub bhcp0518: Option<f64>,
    pub bhcp0520: Option<f64>,
    pub bhcp0522: Option<f64>,
    pub bhcp0533: Option<f64>,
    pub bhcp0534: Option<f64>,
    pub bhcp0536: Option<f64>,
    pub bhcp0537: Option<f64>,
    pub bhcp0538: Option<f64>,
    pub bhcp0539: Option<f64>,
    pub bhcp0540: Option<f64>,
    pub bhcp0541: Option<f64>,
    pub bhcp0542: Option<f64>,
    pub bhcp0543: Option<f64>,
    pub bhcp1273: Option<f64>,
    pub bhcp1274: Option<f64>,
    pub bhcp1275: Option<f64>,
    pub bhcp1276: Option<f64>,
    pub bhcp1277: Option<f64>,
    pub bhcp1278: Option<f64>,
    pub bhcp1279: Option<f64>,
    pub bhcp1299: Option<f64>,
    pub bhcp1403: Option<f64>,
    pub bhcp1407: Option<f64>,
    pub bhcp1616: Option<f64>,
    pub bhcp2123: Option<f64>,
    pub bhcp2125: Option<f64>,
    pub bhcp2145: Option<f64>,
    pub bhcp2160: Option<f64>,
    pub bhcp2165: Option<f64>,
    pub bhcp2170: Option<f64>,
    pub bhcp2200: Option<f64>,
    pub bhcp2309: Option<f64>,
    pub bhcp2332: Option<f64>,
    pub bhcp2792: Option<f64>,
    pub bhcp2793: Option<f64>,
    pub bhcp2794: Option<f64>,
    pub bhcp2796: Option<f64>,
    pub bhcp2831: Option<f64>,
    pub bhcp2930: Option<f64>,
    pub bhcp3123: Option<f64>,
    pub bhcp3128: Option<f64>,
    pub bhcp3147: Option<f64>,
    pub bhcp3152: Option<f64>,
    pub bhcp3153: Option<f64>,
    pub bhcp3156: Option<f64>,
    pub bhcp3163: Option<f64>,
    pub bhcp3164: Option<f64>,
    pub bhcp3165: Option<f64>,
    pub bhcp3210: Option<f64>,
    pub bhcp3230: Option<f64>,
    pub bhcp3238: Option<f64>,
    pub bhcp3239: Option<f64>,
    pub bhcp3240: Option<f64>,
    pub bhcp3247: Option<f64>,
    pub bhcp3283: Option<f64>,
    pub bhcp3290: Option<f64>,
    pub bhcp3293: Option<f64>,
    pub bhcp3298: Option<f64>,
    pub bhcp3300: Option<f64>,
    pub bhcp3409: Option<f64>,
    pub bhcp3513: Option<f64>,
    pub bhcp3602: Option<f64>,
    pub bhcp3603: Option<f64>,
    pub bhcp3604: Option<f64>,
    pub bhcp3605: Option<f64>,
    pub bhcp3606: Option<f64>,
    pub bhcp3607: Option<f64>,
    pub bhcp3609: Option<f64>,
    pub bhcp3611: Option<f64>,
    pub bhcp3612: Option<f64>,
    pub bhcp3613: Option<f64>,
    pub bhcp3614: Option<f64>,
    pub bhcp3615: Option<f64>,
    pub bhcp3616: Option<f64>,
    pub bhcp3617: Option<f64>,
    pub bhcp3618: Option<f64>,
    pub bhcp3619: Option<f64>,
    pub bhcp4000: Option<f64>,
    pub bhcp4062: Option<f64>,
    pub bhcp4073: Option<f64>,
    pub bhcp4091: Option<f64>,
    pub bhcp4130: Option<f64>,
    pub bhcp4135: Option<f64>,
    pub bhcp4230: Option<f64>,
    pub bhcp4243: Option<f64>,
    pub bhcp4250: Option<f64>,
    pub bhcp4302: Option<f64>,
    pub bhcp4320: Option<f64>,
    pub bhcp4336: Option<f64>,
    pub bhcp4340: Option<f64>,
    pub bhcp4485: Option<f64>,
    pub bhcp4605: Option<f64>,
    pub bhcp4635: Option<f64>,
    pub bhcp4647: Option<f64>,
    pub bhcp4778: Option<f64>,
    pub bhcp5485: Option<f64>,
    pub bhcp5486: Option<f64>,
    pub bhcp5487: Option<f64>,
    pub bhcp5488: Option<f64>,
    pub bhcp5489: Option<f64>,
    pub bhcp5993: Option<f64>,
    pub bhcp6552: Option<f64>,
    pub bhcp6567: Option<f64>,
    pub bhcp6571: Option<f64>,
    pub bhcp6573: Option<f64>,
    pub bhcp6588: Option<f64>,
    pub bhcp6589: Option<f64>,
    pub bhcp6590: Option<f64>,
    pub bhcp6591: Option<f64>,
    pub bhcp6592: Option<f64>,
    pub bhcp6596: Option<f64>,
    pub bhcp6600: Option<f64>,
    pub bhcp6604: Option<f64>,
    pub bhcp6607: Option<f64>,
    pub bhcp6619: Option<f64>,
    pub bhcp6649: Option<f64>,
    pub bhcp6741: Option<f64>,
    pub bhcp6742: Option<f64>,
    pub bhcp6743: Option<f64>,
    pub bhcp6744: Option<f64>,
    pub bhcp6758: Option<f64>,
    pub bhcp6773: Option<f64>,
    pub bhcp6775: Option<f64>,
    pub bhcp6791: Option<f64>,
    pub bhcp6792: Option<f64>,
    pub bhcp6793: Option<f64>,
    pub bhcp6794: Option<f64>,
    pub bhcp6795: Option<f64>,
    pub bhcp8434: Option<f64>,
    pub bhcp8516: Option<f64>,
    pub bhcp8517: Option<f64>,
    pub bhcp8518: Option<f64>,
    pub bhcp8843: Option<f64>,
    pub bhcp9191: Option<f64>,
    pub bhcp9802: Option<f64>,
    pub bhcpa130: Option<f64>,
    pub bhcpb530: Option<f64>,
    pub bhcpc254: Option<f64>,
    pub bhcpc255: Option<f64>,
    pub bhcpc427: Option<f64>,
    pub bhcpc428: Option<f64>,
    pub bhcpc447: Option<f64>,
    pub bhcpf229: Option<f64>,
    pub bhcpf737: Option<f64>,
    pub bhcpf817: Option<f64>,
    pub bhcpf818: Option<f64>,
    pub bhcpf819: Option<f64>,
    pub bhcpf820: Option<f64>,
    pub bhcpf838: Option<f64>,
    pub bhcpf841: Option<bool>,
    pub bhcpf842: Option<bool>,
    pub bhcpft28: Option<f64>,
    pub bhcphk02: Option<f64>,
    pub bhcpht69: Option<f64>,
    pub bhcpht70: Option<f64>,
    pub bhcphu25: Option<f64>,
    pub bhcphu26: Option<f64>,
    pub bhcpj980: Option<f64>,
    pub bhcpja22: Option<f64>,
    pub bhcpjj33: Option<f64>,
    pub bhcpk297: Option<f64>,
    pub bhcpky38: Option<bool>,
    pub bhcpm962: Option<f64>,
    pub bhct0426: Option<f64>,
    pub bhct1754: Option<f64>,
    pub bhct1773: Option<f64>,
    pub bhct2143: Option<f64>,
    pub bhct2150: Option<f64>,
    pub bhct2160: Option<f64>,
    pub bhct2170: Option<f64>,
    pub bhct2750: Option<f64>,
    pub bhct3123: Option<f64>,
    pub bhct3190: Option<f64>,
    pub bhct3210: Option<f64>,
    pub bhct3247: Option<f64>,
    pub bhct3368: Option<f64>,
    pub bhct3411: Option<f64>,
    pub bhct3433: Option<f64>,
    pub bhct3543: Option<f64>,
    pub bhct3545: Option<f64>,
    pub bhct3547: Option<f64>,
    pub bhct3548: Option<f64>,
    pub bhct4230: Option<f64>,
    pub bhct4340: Option<f64>,
    pub bhct4605: Option<f64>,
    pub bhct5369: Option<f64>,
    pub bhct5610: Option<f64>,
    pub bhct6570: Option<f64>,
    pub bhcta250: Option<f64>,
    pub bhctb528: Option<f64>,
    pub bhctb590: Option<f64>,
    pub bhctb591: Option<f64>,
    pub bhcw3792: Option<f64>,
    pub bhcw5310: Option<f64>,
    pub bhcw5311: Option<f64>,
    pub bhcw7205: Option<f64>,
    pub bhcw7206: Option<f64>,
    pub bhcwa223: Option<f64>,
    pub bhcwh311: Option<f64>,
    pub bhcwkx78: Option<bool>,
    pub bhcwkx83: Option<bool>,
    pub bhcwle85: Option<f64>,
    pub bhcwle86: Option<f64>,
    pub bhcwle87: Option<f64>,
    pub bhcwlf23: Option<f64>,
    pub bhcwlf24: Option<f64>,
    pub bhcwlf25: Option<f64>,
    pub bhcwmk66: Option<f64>,
    pub bhcwp793: Option<f64>,
    pub bhcwp851: Option<f64>,
    pub bhcwp852: Option<f64>,
    pub bhcwp853: Option<f64>,
    pub bhcwp854: Option<f64>,
    pub bhcwp855: Option<f64>,
    pub bhcwp856: Option<f64>,
    pub bhcwp857: Option<f64>,
    pub bhcwp858: Option<f64>,
    pub bhcwp859: Option<f64>,
    pub bhcwp870: Option<f64>,
    pub bhcx1754: Option<f64>,
    pub bhcx1773: Option<f64>,
    pub bhcx3123: Option<f64>,
    pub bhcx3210: Option<f64>,
    pub bhcx3368: Option<f64>,
    pub bhcx3545: Option<f64>,
    pub bhcy1773: Option<f64>,
    pub bhcy3123: Option<f64>,
    pub bhcyja36: Option<f64>,
    pub bhdm1288: Option<f64>,
    pub bhdm1410: Option<f64>,
    pub bhdm1415: Option<f64>,
    pub bhdm1420: Option<f64>,
    pub bhdm1460: Option<f64>,
    pub bhdm1480: Option<f64>,
    pub bhdm1545: Option<f64>,
    pub bhdm1564: Option<f64>,
    pub bhdm1590: Option<f64>,
    pub bhdm1635: Option<f64>,
    pub bhdm1755: Option<f64>,
    pub bhdm1766: Option<f64>,
    pub bhdm1797: Option<f64>,
    pub bhdm1975: Option<f64>,
    pub bhdm2081: Option<f64>,
    pub bhdm2122: Option<f64>,
    pub bhdm2123: Option<f64>,
    pub bhdm2165: Option<f64>,
    pub bhdm3386: Option<f64>,
    pub bhdm3387: Option<f64>,
    pub bhdm3465: Option<f64>,
    pub bhdm3466: Option<f64>,
    pub bhdm3516: Option<f64>,
    pub bhdm3545: Option<f64>,
    pub bhdm3546: Option<f64>,
    pub bhdm3547: Option<f64>,
    pub bhdm3548: Option<f64>,
    pub bhdm5367: Option<f64>,
    pub bhdm5368: Option<f64>,
    pub bhdm6631: Option<f64>,
    pub bhdm6636: Option<f64>,
    pub bhdma164: Option<f64>,
    pub bhdma242: Option<f64>,
    pub bhdma243: Option<f64>,
    pub bhdmb561: Option<f64>,
    pub bhdmb562: Option<f64>,
    pub bhdmb987: Option<f64>,
    pub bhdmb993: Option<f64>,
    pub bhdmf560: Option<f64>,
    pub bhdmf576: Option<f64>,
    pub bhdmf577: Option<f64>,
    pub bhdmf578: Option<f64>,
    pub bhdmf579: Option<f64>,
    pub bhdmf580: Option<f64>,
    pub bhdmf581: Option<f64>,
    pub bhdmf582: Option<f64>,
    pub bhdmf583: Option<f64>,
    pub bhdmf584: Option<f64>,
    pub bhdmf585: Option<f64>,
    pub bhdmf586: Option<f64>,
    pub bhdmf587: Option<f64>,
    pub bhdmf588: Option<f64>,
    pub bhdmf589: Option<f64>,
    pub bhdmf590: Option<f64>,
    pub bhdmf591: Option<f64>,
    pub bhdmf592: Option<f64>,
    pub bhdmf593: Option<f64>,
    pub bhdmf594: Option<f64>,
    pub bhdmf595: Option<f64>,
    pub bhdmf596: Option<f64>,
    pub bhdmf597: Option<f64>,
    pub bhdmf598: Option<f64>,
    pub bhdmf599: Option<f64>,
    pub bhdmf600: Option<f64>,
    pub bhdmf601: Option<f64>,
    pub bhdmf604: Option<f64>,
    pub bhdmf605: Option<f64>,
    pub bhdmf606: Option<f64>,
    pub bhdmf607: Option<f64>,
    pub bhdmf611: Option<f64>,
    pub bhdmf612: Option<f64>,
    pub bhdmf613: Option<f64>,
    pub bhdmf614: Option<f64>,
    pub bhdmf615: Option<f64>,
    pub bhdmf616: Option<f64>,
    pub bhdmf617: Option<f64>,
    pub bhdmf618: Option<f64>,
    pub bhdmf624: Option<f64>,
    pub bhdmf625: Option<f64>,
    pub bhdmf626: Option<f64>,
    pub bhdmf627: Option<f64>,
    pub bhdmf628: Option<f64>,
    pub bhdmf629: Option<f64>,
    pub bhdmf630: Option<f64>,
    pub bhdmf631: Option<f64>,
    pub bhdmf632: Option<f64>,
    pub bhdmf633: Option<f64>,
    pub bhdmf634: Option<f64>,
    pub bhdmf635: Option<f64>,
    pub bhdmf636: Option<f64>,
    pub bhdmf639: Option<f64>,
    pub bhdmf640: Option<f64>,
    pub bhdmf670: Option<f64>,
    pub bhdmf671: Option<f64>,
    pub bhdmf672: Option<f64>,
    pub bhdmf673: Option<f64>,
    pub bhdmf674: Option<f64>,
    pub bhdmf675: Option<f64>,
    pub bhdmf676: Option<f64>,
    pub bhdmf677: Option<f64>,
    pub bhdmf678: Option<f64>,
    pub bhdmf679: Option<f64>,
    pub bhdmf680: Option<f64>,
    pub bhdmf681: Option<f64>,
    pub bhdmf724: Option<f64>,
    pub bhdmg209: Option<f64>,
    pub bhdmg210: Option<f64>,
    pub bhdmg211: Option<f64>,
    pub bhdmg299: Option<f64>,
    pub bhdmg332: Option<f64>,
    pub bhdmg333: Option<f64>,
    pub bhdmg334: Option<f64>,
    pub bhdmg335: Option<f64>,
    pub bhdmg379: Option<f64>,
    pub bhdmg380: Option<f64>,
    pub bhdmg381: Option<f64>,
    pub bhdmg382: Option<f64>,
    pub bhdmg383: Option<f64>,
    pub bhdmg384: Option<f64>,
    pub bhdmg385: Option<f64>,
    pub bhdmg386: Option<f64>,
    pub bhdmg387: Option<f64>,
    pub bhdmg388: Option<f64>,
    pub bhdmg651: Option<f64>,
    pub bhdmg652: Option<f64>,
    pub bhdmhk06: Option<f64>,
    pub bhdmhk31: Option<f64>,
    pub bhdmhk32: Option<f64>,
    pub bhdmj451: Option<f64>,
    pub bhdmj454: Option<f64>,
    pub bhdmk045: Option<f64>,
    pub bhdmk046: Option<f64>,
    pub bhdmk047: Option<f64>,
    pub bhdmk048: Option<f64>,
    pub bhdmk049: Option<f64>,
    pub bhdmk050: Option<f64>,
    pub bhdmk051: Option<f64>,
    pub bhdmk052: Option<f64>,
    pub bhdmk053: Option<f64>,
    pub bhdmk054: Option<f64>,
    pub bhdmk055: Option<f64>,
    pub bhdmk056: Option<f64>,
    pub bhdmk057: Option<f64>,
    pub bhdmk058: Option<f64>,
    pub bhdmk059: Option<f64>,
    pub bhdmk060: Option<f64>,
    pub bhdmk061: Option<f64>,
    pub bhdmk062: Option<f64>,
    pub bhdmk063: Option<f64>,
    pub bhdmk064: Option<f64>,
    pub bhdmk065: Option<f64>,
    pub bhdmk066: Option<f64>,
    pub bhdmk067: Option<f64>,
    pub bhdmk068: Option<f64>,
    pub bhdmk069: Option<f64>,
    pub bhdmk070: Option<f64>,
    pub bhdmk071: Option<f64>,
    pub bhdmk105: Option<f64>,
    pub bhdmk106: Option<f64>,
    pub bhdmk107: Option<f64>,
    pub bhdmk108: Option<f64>,
    pub bhdmk109: Option<f64>,
    pub bhdmk110: Option<f64>,
    pub bhdmk111: Option<f64>,
    pub bhdmk112: Option<f64>,
    pub bhdmk113: Option<f64>,
    pub bhdmk114: Option<f64>,
    pub bhdmk115: Option<f64>,
    pub bhdmk116: Option<f64>,
    pub bhdmk117: Option<f64>,
    pub bhdmk118: Option<f64>,
    pub bhdmk119: Option<f64>,
    pub bhdmk130: Option<f64>,
    pub bhdmk131: Option<f64>,
    pub bhdmk132: Option<f64>,
    pub bhdmk158: Option<f64>,
    pub bhdmk159: Option<f64>,
    pub bhdmk160: Option<f64>,
    pub bhdmk161: Option<f64>,
    pub bhdmk162: Option<f64>,
    pub bhdmk166: Option<f64>,
    pub bhdmk169: Option<f64>,
    pub bhdmk170: Option<f64>,
    pub bhdmk171: Option<f64>,
    pub bhdmk172: Option<f64>,
    pub bhdmk173: Option<f64>,
    pub bhdmk174: Option<f64>,
    pub bhdmk175: Option<f64>,
    pub bhdmk176: Option<f64>,
    pub bhdmk177: Option<f64>,
    pub bhdmk187: Option<f64>,
    pub bhdmk188: Option<f64>,
    pub bhdmk189: Option<f64>,
    pub bhdmk190: Option<f64>,
    pub bhdmk191: Option<f64>,
    pub bhdmk195: Option<f64>,
    pub bhdmk196: Option<f64>,
    pub bhdmk197: Option<f64>,
    pub bhdmk198: Option<f64>,
    pub bhdmk199: Option<f64>,
    pub bhdmk200: Option<f64>,
    pub bhdmk208: Option<f64>,
    pub bhdmk209: Option<f64>,
    pub bhdmk210: Option<f64>,
    pub bhdmk211: Option<f64>,
    pub bhdmkx57: Option<f64>,
    pub bhfn3360: Option<f64>,
    pub bhfn3543: Option<f64>,
    pub bhfn6631: Option<f64>,
    pub bhfn6636: Option<f64>,
    pub bhfna245: Option<f64>,
    pub bhfnk260: Option<f64>,
    pub bhod2389: Option<f64>,
    pub bhod2604: Option<f64>,
    pub bhod3187: Option<f64>,
    pub bhod3189: Option<f64>,
    pub bhod6648: Option<f64>,
    pub bhodhk29: Option<f64>,
    pub bhodj474: Option<f64>,
    pub bhpa0365: Option<f64>,
    pub bhpa4340: Option<f64>,
    pub bhpx8901: Option<String>,
    pub bhsp0010: Option<f64>,
    pub bhsp0027: Option<f64>,
    pub bhsp0087: Option<f64>,
    pub bhsp0088: Option<f64>,
    pub bhsp0089: Option<f64>,
    pub bhsp0201: Option<f64>,
    pub bhsp0202: Option<f64>,
    pub bhsp0206: Option<f64>,
    pub bhsp0390: Option<f64>,
    pub bhsp0416: Option<f64>,
    pub bhsp0447: Option<f64>,
    pub bhsp0496: Option<f64>,
    pub bhsp0508: Option<f64>,
    pub bhsp0523: Option<f64>,
    pub bhsp0530: Option<f64>,
    pub bhsp1283: Option<f64>,
    pub bhsp2111: Option<f64>,
    pub bhsp2112: Option<f64>,
    pub bhsp2122: Option<f64>,
    pub bhsp2145: Option<f64>,
    pub bhsp2148: Option<f64>,
    pub bhsp2170: Option<f64>,
    pub bhsp2309: Option<f64>,
    pub bhsp2723: Option<f64>,
    pub bhsp2724: Option<f64>,
    pub bhsp2792: Option<f64>,
    pub bhsp2794: Option<f64>,
    pub bhsp2796: Option<f64>,
    pub bhsp2932: Option<f64>,
    pub bhsp3049: Option<f64>,
    pub bhsp3066: Option<f64>,
    pub bhsp3123: Option<f64>,
    pub bhsp3148: Option<f64>,
    pub bhsp3151: Option<f64>,
    pub bhsp3152: Option<f64>,
    pub bhsp3153: Option<f64>,
    pub bhsp3154: Option<f64>,
    pub bhsp3155: Option<f64>,
    pub bhsp3156: Option<f64>,
    pub bhsp3158: Option<f64>,
    pub bhsp3166: Option<f64>,
    pub bhsp3167: Option<f64>,
    pub bhsp3210: Option<f64>,
    pub bhsp3230: Option<f64>,
    pub bhsp3238: Option<f64>,
    pub bhsp3239: Option<f64>,
    pub bhsp3247: Option<f64>,
    pub bhsp3283: Option<f64>,
    pub bhsp3300: Option<f64>,
    pub bhsp3513: Option<f64>,
    pub bhsp3523: Option<f64>,
    pub bhsp3524: Option<f64>,
    pub bhsp3525: Option<f64>,
    pub bhsp3526: Option<f64>,
    pub bhsp3527: Option<f64>,
    pub bhsp3605: Option<f64>,
    pub bhsp3620: Option<f64>,
    pub bhsp3621: Option<f64>,
    pub bhsp4000: Option<f64>,
    pub bhsp4073: Option<f64>,
    pub bhsp4093: Option<f64>,
    pub bhsp4130: Option<f64>,
    pub bhsp4250: Option<f64>,
    pub bhsp4302: Option<f64>,
    pub bhsp4336: Option<f64>,
    pub bhsp4340: Option<f64>,
    pub bhsp4778: Option<f64>,
    pub bhsp5993: Option<f64>,
    pub bhsp6416: Option<f64>,
    pub bhsp6649: Option<f64>,
    pub bhsp6796: Option<f64>,
    pub bhsp6797: Option<f64>,
    pub bhsp8434: Option<f64>,
    pub bhsp8516: Option<f64>,
    pub bhsp8517: Option<f64>,
    pub bhsp8519: Option<f64>,
    pub bhsp8520: Option<f64>,
    pub bhsp8521: Option<f64>,
    pub bhsp8522: Option<f64>,
    pub bhsp8523: Option<f64>,
    pub bhsp8524: Option<f64>,
    pub bhsp8525: Option<f64>,
    pub bhsp8526: Option<f64>,
    pub bhsp8527: Option<f64>,
    pub bhsp8528: Option<f64>,
    pub bhsp8529: Option<f64>,
    pub bhsp8530: Option<f64>,
    pub bhsp8843: Option<f64>,
    pub bhsp9191: Option<f64>,
    pub bhsp9802: Option<f64>,
    pub bhspa024: Option<f64>,
    pub bhspa130: Option<f64>,
    pub bhspa530: Option<f64>,
    pub bhspb530: Option<f64>,
    pub bhspc009: Option<f64>,
    pub bhspc159: Option<f64>,
    pub bhspc160: Option<bool>,
    pub bhspc161: Option<f64>,
    pub bhspc252: Option<f64>,
    pub bhspc253: Option<f64>,
    pub bhspc254: Option<f64>,
    pub bhspc255: Option<f64>,
    pub bhspc256: Option<f64>,
    pub bhspc257: Option<f64>,
    pub bhspc427: Option<f64>,
    pub bhspc428: Option<f64>,
    pub bhspc447: Option<f64>,
    pub bhspc700: Option<f64>,
    pub bhspc701: Option<f64>,
    pub bhspc702: Option<f64>,
    pub bhspc884: Option<f64>,
    pub bhspf074: Option<f64>,
    pub bhspf075: Option<f64>,
    pub bhspf229: Option<f64>,
    pub bhspf819: Option<f64>,
    pub bhspf820: Option<f64>,
    pub bhspf838: Option<f64>,
    pub bhspf841: Option<bool>,
    pub bhspf842: Option<bool>,
    pub bhspft28: Option<f64>,
    pub bhspft42: Option<bool>,
    pub bhspft43: Option<bool>,
    pub bhspft44: Option<bool>,
    pub bhspg234: Option<f64>,
    pub bhspg235: Option<f64>,
    pub bhspht69: Option<f64>,
    pub bhspht70: Option<f64>,
    pub bhspht95: Option<f64>,
    pub bhspj980: Option<f64>,
    pub bhspk141: Option<f64>,
    pub bhspky38: Option<bool>,
    pub bhspm962: Option<f64>,
    pub bhspmz36: Option<f64>,
    pub bhspnk60: Option<bool>,
    pub bhsx8901: Option<String>,
    pub bhtxf655: Option<String>,
    pub bhtxf656: Option<String>,
    pub bhtxf657: Option<String>,
    pub bhtxf658: Option<String>,
    pub bhtxf659: Option<String>,
    pub bhtxf660: Option<String>,
    pub bhtxg546: Option<String>,
    pub bhtxg551: Option<String>,
    pub bhtxg556: Option<String>,
    pub bhtxg561: Option<String>,
    pub bhtxg571: Option<String>,
    pub bhtxg576: Option<bool>,
    pub bhtxg581: Option<String>,
    pub bhtxg586: Option<String>,
    pub rssd4087: Option<String>,
    pub rssd6191: Option<f64>,
    pub rssd9001: Option<f64>, // Option<i64>,
    pub rssd9005: Option<String>,
    pub rssd9007: Option<NaiveDate>,
    pub rssd9008: Option<NaiveDate>,
    pub rssd9010: Option<String>,
    pub rssd9014: Option<f64>,
    pub rssd9016: Option<f64>,
    pub rssd9017: Option<String>,
    pub rssd9028: Option<String>,
    pub rssd9029: Option<String>,
    pub rssd9030: Option<f64>,
    pub rssd9031: Option<f64>,
    pub rssd9032: Option<f64>,
    pub rssd9037: Option<f64>,
    pub rssd9038: Option<String>,
    pub rssd9039: Option<f64>,
    pub rssd9042: Option<f64>,
    pub rssd9044: Option<f64>,
    pub rssd9045: Option<f64>,
    pub rssd9046: Option<f64>,
    pub rssd9047: Option<f64>,
    pub rssd9048: Option<f64>,
    pub rssd9049: Option<f64>,
    pub rssd9050: Option<f64>,
    pub rssd9052: Option<f64>,
    pub rssd9053: Option<f64>,
    pub rssd9054: Option<f64>,
    pub rssd9055: Option<f64>,
    pub rssd9056: Option<f64>,
    pub rssd9059: Option<f64>,
    pub rssd9060: Option<f64>,
    pub rssd9061: Option<f64>,
    pub rssd9101: Option<String>,
    pub rssd9130: Option<String>,
    pub rssd9132: Option<f64>,
    pub rssd9138: Option<f64>,
    pub rssd9146: Option<f64>,
    pub rssd9150: Option<f64>,
    pub rssd9161: Option<String>,
    pub rssd9170: Option<f64>,
    pub rssd9192: Option<String>,
    pub rssd9198: Option<f64>,
    pub rssd9200: Option<String>,
    pub rssd9210: Option<f64>,
    pub rssd9213: Option<f64>,
    pub rssd9216: Option<f64>,
    pub rssd9220: Option<String>,
    pub rssd9320: Option<f64>,
    pub rssd9374: Option<f64>,
    pub rssd9375: Option<f64>,
    pub rssd9421: Option<f64>,
    pub rssd9422: Option<f64>,
    pub rssd9424: Option<f64>,
    pub rssd9425: Option<f64>,
    pub rssd9579: Option<f64>,
    pub rssd9950: Option<NaiveDate>,
    pub rssd9955: Option<f64>,
    pub rssd9999: Option<f64>, // Option<NaiveDate>,
    pub texc3573: Option<bool>,
    pub texc3575: Option<bool>,
    pub texc6373: Option<f64>,
    pub texc6561: Option<bool>,
    pub texc6562: Option<bool>,
    pub texc6568: Option<bool>,
    pub texc6586: Option<bool>,
    pub texc6995: Option<bool>,
    pub texc6996: Option<bool>,
    pub texc6997: Option<bool>,
    pub texc6998: Option<bool>,
    pub texc8520: Option<f64>,
    pub texc8521: Option<bool>,
    pub texc8522: Option<bool>,
    pub texc8523: Option<f64>,
    pub texc8524: Option<f64>,
    pub texc8525: Option<bool>,
    pub texc8557: Option<f64>,
    pub texc8558: Option<f64>,
    pub texc8559: Option<f64>,
    pub texc8562: Option<f64>,
    pub texc8563: Option<f64>,
    pub texc8564: Option<f64>,
    pub texc8565: Option<f64>,
    pub texc8566: Option<f64>,
    pub texc8567: Option<f64>,
    pub text3571: Option<String>,
    pub text3573: Option<String>,
    pub text3575: Option<String>,
    pub text4769: Option<bool>,
    pub text5351: Option<String>,
    pub text5352: Option<String>,
    pub text5353: Option<String>,
    pub text5354: Option<String>,
    pub text5355: Option<String>,
    pub text5356: Option<String>,
    pub text5357: Option<String>,
    pub text5358: Option<String>,
    pub text5359: Option<String>,
    pub text5360: Option<String>,
    pub text5485: Option<String>,
    pub text5486: Option<String>,
    pub text5487: Option<String>,
    pub text5488: Option<String>,
    pub text5489: Option<String>,
    pub text5523: Option<bool>,
    pub text6373: Option<String>,
    pub text6561: Option<String>,
    pub text6562: Option<String>,
    pub text6568: Option<String>,
    pub text6586: Option<String>,
    pub text6995: Option<bool>,
    pub text6996: Option<bool>,
    pub text6997: Option<bool>,
    pub text6998: Option<bool>,
    pub text8520: Option<String>,
    pub text8521: Option<String>,
    pub text8522: Option<String>,
    pub text8523: Option<String>,
    pub text8524: Option<String>,
    pub text8525: Option<String>,
    pub text8526: Option<String>,
    pub text8527: Option<String>,
    pub text8528: Option<String>,
    pub text8529: Option<String>,
    pub text8530: Option<String>,
    pub text8557: Option<String>,
    pub text8558: Option<String>,
    pub text8559: Option<String>,
    pub text8562: Option<String>,
    pub text8563: Option<String>,
    pub text8564: Option<String>,
    pub text8565: Option<String>,
    pub text8566: Option<String>,
    pub text8567: Option<String>,
    pub textb027: Option<String>,
    pub textb028: Option<String>,
    pub textb029: Option<String>,
    pub textb030: Option<String>,
    pub textb031: Option<String>,
    pub textb032: Option<String>,
    pub textb033: Option<String>,
    pub textb034: Option<String>,
    pub textb035: Option<String>,
    pub textb036: Option<String>,
    pub textb037: Option<String>,
    pub textb038: Option<String>,
    pub textb039: Option<String>,
    pub textb040: Option<String>,
    pub textb041: Option<String>,
    pub textb042: Option<String>,
    pub textb043: Option<String>,
    pub textb044: Option<String>,
    pub textb045: Option<String>,
    pub textb046: Option<String>,
    pub textb047: Option<String>,
    pub textb048: Option<String>,
    pub textb049: Option<String>,
    pub textb050: Option<String>,
    pub textb051: Option<String>,
    pub textb052: Option<String>,
    pub textb053: Option<String>,
    pub textb054: Option<bool>,
    pub textb055: Option<bool>,
    pub textb056: Option<String>,
    pub textc231: Option<String>,
    pub textc490: Option<bool>,
    pub textc497: Option<String>,
    pub textc703: Option<String>,
    pub textc708: Option<String>,
    pub textc714: Option<String>,
    pub textc715: Option<String>,
    pub textft29: Option<bool>,
    pub textft31: Option<String>,
    pub wrdsdownloaddate: Option<NaiveDate>,
}

impl SurrealCrudModel for BhckOther1 {
    fn table() -> &'static str {
        "bhck_other"
    }
    fn id_key(&self) -> Option<String> {
        match (self.rssd9001, self.rssd9999) {
            (Some(rssd9001), Some(rssd9999)) => Some(format!("{rssd9001}:{rssd9999}")),
            _ => None,
        }
    }
}

impl DuckCrudModel for BhckOther1 {
    fn table() -> &'static str {
        "bhck_other"
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl ToPolars for BhckOther1 {
    fn schema() -> Schema {
        BhckOther1::polars_schema()
    }
}

impl BhckOther1 {
    pub fn polars_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("bhbc3368".into(), DataType::Float64),
            Field::new("bhbc3402".into(), DataType::Float64),
            Field::new("bhbc3516".into(), DataType::Float64),
            Field::new("bhbc3519".into(), DataType::Float64),
            Field::new("bhbc4070".into(), DataType::Float64),
            Field::new("bhbc4073".into(), DataType::Float64),
            Field::new("bhbc4074".into(), DataType::Float64),
            Field::new("bhbc4079".into(), DataType::Float64),
            Field::new("bhbc4091".into(), DataType::Float64),
            Field::new("bhbc4093".into(), DataType::Float64),
            Field::new("bhbc4094".into(), DataType::Float64),
            Field::new("bhbc4107".into(), DataType::Float64),
            Field::new("bhbc4135".into(), DataType::Float64),
            Field::new("bhbc4218".into(), DataType::Float64),
            Field::new("bhbc4230".into(), DataType::Float64),
            Field::new("bhbc4301".into(), DataType::Float64),
            Field::new("bhbc4302".into(), DataType::Float64),
            Field::new("bhbc4320".into(), DataType::Float64),
            Field::new("bhbc4340".into(), DataType::Float64),
            Field::new("bhbc4421".into(), DataType::Float64),
            Field::new("bhbc4475".into(), DataType::Float64),
            Field::new("bhbc4484".into(), DataType::Float64),
            Field::new("bhbc4519".into(), DataType::Float64),
            Field::new("bhbc6061".into(), DataType::Float64),
            Field::new("bhbca220".into(), DataType::Float64),
            Field::new("bhbcb490".into(), DataType::Float64),
            Field::new("bhbcb491".into(), DataType::Float64),
            Field::new("bhbcb493".into(), DataType::Float64),
            Field::new("bhbcb494".into(), DataType::Float64),
            Field::new("bhbcc216".into(), DataType::Float64),
            Field::new("bhbcjj33".into(), DataType::Float64),
            Field::new("bhc00010".into(), DataType::Float64),
            Field::new("bhc00390".into(), DataType::Float64),
            Field::new("bhc01350".into(), DataType::Float64),
            Field::new("bhc01754".into(), DataType::Float64),
            Field::new("bhc01773".into(), DataType::Float64),
            Field::new("bhc02122".into(), DataType::Float64),
            Field::new("bhc02170".into(), DataType::Float64),
            Field::new("bhc03411".into(), DataType::Float64),
            Field::new("bhc03429".into(), DataType::Float64),
            Field::new("bhc03433".into(), DataType::Float64),
            Field::new("bhc03545".into(), DataType::Float64),
            Field::new("bhc05369".into(), DataType::Float64),
            Field::new("bhc06551".into(), DataType::Float64),
            Field::new("bhc06563".into(), DataType::Float64),
            Field::new("bhc06566".into(), DataType::Float64),
            Field::new("bhc06570".into(), DataType::Float64),
            Field::new("bhc06572".into(), DataType::Float64),
            Field::new("bhc06574".into(), DataType::Float64),
            Field::new("bhc06575".into(), DataType::Float64),
            Field::new("bhc06598".into(), DataType::Float64),
            Field::new("bhc06601".into(), DataType::Boolean),
            Field::new("bhc06602".into(), DataType::Float64),
            Field::new("bhc06603".into(), DataType::Float64),
            Field::new("bhc0a167".into(), DataType::Float64),
            Field::new("bhc0a250".into(), DataType::Float64),
            Field::new("bhc0b528".into(), DataType::Float64),
            Field::new("bhc0b546".into(), DataType::Float64),
            Field::new("bhc0b639".into(), DataType::Float64),
            Field::new("bhc0b675".into(), DataType::Float64),
            Field::new("bhc0b681".into(), DataType::Float64),
            Field::new("bhc0c225".into(), DataType::Float64),
            Field::new("bhc0g591".into(), DataType::Float64),
            Field::new("bhc20010".into(), DataType::Float64),
            Field::new("bhc20390".into(), DataType::Float64),
            Field::new("bhc21350".into(), DataType::Float64),
            Field::new("bhc21754".into(), DataType::Float64),
            Field::new("bhc21773".into(), DataType::Float64),
            Field::new("bhc22122".into(), DataType::Float64),
            Field::new("bhc22170".into(), DataType::Float64),
            Field::new("bhc23411".into(), DataType::Float64),
            Field::new("bhc23429".into(), DataType::Float64),
            Field::new("bhc23433".into(), DataType::Float64),
            Field::new("bhc23545".into(), DataType::Float64),
            Field::new("bhc25369".into(), DataType::Float64),
            Field::new("bhc26551".into(), DataType::Float64),
            Field::new("bhc26563".into(), DataType::Float64),
            Field::new("bhc26566".into(), DataType::Float64),
            Field::new("bhc26570".into(), DataType::Float64),
            Field::new("bhc26572".into(), DataType::Float64),
            Field::new("bhc26574".into(), DataType::Float64),
            Field::new("bhc26575".into(), DataType::Float64),
            Field::new("bhc26598".into(), DataType::Float64),
            Field::new("bhc26601".into(), DataType::Float64),
            Field::new("bhc26602".into(), DataType::Float64),
            Field::new("bhc26603".into(), DataType::Float64),
            Field::new("bhc2a167".into(), DataType::Float64),
            Field::new("bhc2a250".into(), DataType::Float64),
            Field::new("bhc2b528".into(), DataType::Float64),
            Field::new("bhc2b546".into(), DataType::Float64),
            Field::new("bhc2b639".into(), DataType::Float64),
            Field::new("bhc2b675".into(), DataType::Float64),
            Field::new("bhc2b681".into(), DataType::Float64),
            Field::new("bhc2c225".into(), DataType::Float64),
            Field::new("bhc2g591".into(), DataType::Float64),
            Field::new("bhc50390".into(), DataType::Float64),
            Field::new("bhc51350".into(), DataType::Boolean),
            Field::new("bhc51754".into(), DataType::Float64),
            Field::new("bhc51773".into(), DataType::Float64),
            Field::new("bhc52122".into(), DataType::Float64),
            Field::new("bhc52170".into(), DataType::Float64),
            Field::new("bhc53411".into(), DataType::Float64),
            Field::new("bhc53433".into(), DataType::Float64),
            Field::new("bhc53545".into(), DataType::Float64),
            Field::new("bhc55369".into(), DataType::Float64),
            Field::new("bhc56551".into(), DataType::Float64),
            Field::new("bhc56563".into(), DataType::Float64),
            Field::new("bhc56566".into(), DataType::Float64),
            Field::new("bhc56570".into(), DataType::Float64),
            Field::new("bhc56572".into(), DataType::Float64),
            Field::new("bhc56574".into(), DataType::Float64),
            Field::new("bhc56575".into(), DataType::Float64),
            Field::new("bhc56598".into(), DataType::Float64),
            Field::new("bhc56602".into(), DataType::Float64),
            Field::new("bhc56603".into(), DataType::Float64),
            Field::new("bhc5a167".into(), DataType::Float64),
            Field::new("bhc5a250".into(), DataType::Float64),
            Field::new("bhc5b528".into(), DataType::Float64),
            Field::new("bhc5b546".into(), DataType::Float64),
            Field::new("bhc5b639".into(), DataType::Float64),
            Field::new("bhc5b675".into(), DataType::Float64),
            Field::new("bhc5b681".into(), DataType::Float64),
            Field::new("bhc5g591".into(), DataType::Float64),
            Field::new("bhc90010".into(), DataType::Float64),
            Field::new("bhc90390".into(), DataType::Float64),
            Field::new("bhc91350".into(), DataType::Float64),
            Field::new("bhc91727".into(), DataType::Float64),
            Field::new("bhc91754".into(), DataType::Float64),
            Field::new("bhc91773".into(), DataType::Float64),
            Field::new("bhc92122".into(), DataType::Float64),
            Field::new("bhc92170".into(), DataType::Float64),
            Field::new("bhc93411".into(), DataType::Float64),
            Field::new("bhc93429".into(), DataType::Float64),
            Field::new("bhc93433".into(), DataType::Float64),
            Field::new("bhc93545".into(), DataType::Float64),
            Field::new("bhc95369".into(), DataType::Float64),
            Field::new("bhc96551".into(), DataType::Float64),
            Field::new("bhc96563".into(), DataType::Float64),
            Field::new("bhc96566".into(), DataType::Float64),
            Field::new("bhc96570".into(), DataType::Float64),
            Field::new("bhc96572".into(), DataType::Float64),
            Field::new("bhc96574".into(), DataType::Float64),
            Field::new("bhc96575".into(), DataType::Float64),
            Field::new("bhc96598".into(), DataType::Float64),
            Field::new("bhc96602".into(), DataType::Float64),
            Field::new("bhc96603".into(), DataType::Float64),
            Field::new("bhc9a250".into(), DataType::Float64),
            Field::new("bhc9b528".into(), DataType::Float64),
            Field::new("bhc9b541".into(), DataType::Float64),
            Field::new("bhc9b546".into(), DataType::Float64),
            Field::new("bhc9b639".into(), DataType::Float64),
            Field::new("bhc9b675".into(), DataType::Float64),
            Field::new("bhc9b681".into(), DataType::Float64),
            Field::new("bhc9c225".into(), DataType::Float64),
            Field::new("bhc9g591".into(), DataType::Float64),
            Field::new("bhca2170".into(), DataType::Float64),
            Field::new("bhca3792".into(), DataType::Float64),
            Field::new("bhca5310".into(), DataType::Float64),
            Field::new("bhca5311".into(), DataType::Float64),
            Field::new("bhca7204".into(), DataType::Float64),
            Field::new("bhca7205".into(), DataType::Float64),
            Field::new("bhca7206".into(), DataType::Float64),
            Field::new("bhca8274".into(), DataType::Float64),
            Field::new("bhcaa223".into(), DataType::Float64),
            Field::new("bhcaa224".into(), DataType::Float64),
            Field::new("bhcab530".into(), DataType::Float64),
            Field::new("bhcab596".into(), DataType::Float64),
            Field::new("bhcah036".into(), DataType::Float64),
            Field::new("bhcah311".into(), DataType::Float64),
            Field::new("bhcah312".into(), DataType::Boolean),
            Field::new("bhcah313".into(), DataType::Float64),
            Field::new("bhcah314".into(), DataType::Float64),
            Field::new("bhcajj29".into(), DataType::Float64),
            Field::new("bhcakw00".into(), DataType::Float64),
            Field::new("bhcakw03".into(), DataType::Float64),
            Field::new("bhcakx77".into(), DataType::Float64),
            Field::new("bhcakx78".into(), DataType::Float64),
            Field::new("bhcakx79".into(), DataType::Float64),
            Field::new("bhcakx80".into(), DataType::Float64),
            Field::new("bhcakx81".into(), DataType::Float64),
            Field::new("bhcakx82".into(), DataType::Float64),
            Field::new("bhcakx83".into(), DataType::Float64),
            Field::new("bhcalb58".into(), DataType::Float64),
            Field::new("bhcalb59".into(), DataType::Float64),
            Field::new("bhcalb60".into(), DataType::Float64),
            Field::new("bhcalb61".into(), DataType::Float64),
            Field::new("bhcale74".into(), DataType::Float64),
            Field::new("bhcale85".into(), DataType::Float64),
            Field::new("bhcale86".into(), DataType::Float64),
            Field::new("bhcale87".into(), DataType::Float64),
            Field::new("bhcale88".into(), DataType::Float64),
            Field::new("bhcale89".into(), DataType::Float64),
            Field::new("bhcale90".into(), DataType::Float64),
            Field::new("bhcale91".into(), DataType::Float64),
            Field::new("bhcale92".into(), DataType::Float64),
            Field::new("bhcalf21".into(), DataType::Float64),
            Field::new("bhcalf22".into(), DataType::Float64),
            Field::new("bhcalf23".into(), DataType::Float64),
            Field::new("bhcalf24".into(), DataType::Float64),
            Field::new("bhcalf25".into(), DataType::Float64),
            Field::new("bhcalf27".into(), DataType::Float64),
            Field::new("bhcalf28".into(), DataType::Float64),
            Field::new("bhcamk66".into(), DataType::Float64),
            Field::new("bhcamk76".into(), DataType::Float64),
            Field::new("bhcamk77".into(), DataType::Float64),
            Field::new("bhcamk78".into(), DataType::Float64),
            Field::new("bhcanc99".into(), DataType::Float64),
            Field::new("bhcap742".into(), DataType::Float64),
            Field::new("bhcap793".into(), DataType::Float64),
            Field::new("bhcap838".into(), DataType::Float64),
            Field::new("bhcap839".into(), DataType::Float64),
            Field::new("bhcap840".into(), DataType::Float64),
            Field::new("bhcap841".into(), DataType::Float64),
            Field::new("bhcap842".into(), DataType::Float64),
            Field::new("bhcap843".into(), DataType::Float64),
            Field::new("bhcap844".into(), DataType::Float64),
            Field::new("bhcap845".into(), DataType::Float64),
            Field::new("bhcap846".into(), DataType::Float64),
            Field::new("bhcap847".into(), DataType::Float64),
            Field::new("bhcap848".into(), DataType::Float64),
            Field::new("bhcap849".into(), DataType::Float64),
            Field::new("bhcap850".into(), DataType::Float64),
            Field::new("bhcap851".into(), DataType::Float64),
            Field::new("bhcap852".into(), DataType::Float64),
            Field::new("bhcap853".into(), DataType::Float64),
            Field::new("bhcap854".into(), DataType::Float64),
            Field::new("bhcap855".into(), DataType::Float64),
            Field::new("bhcap856".into(), DataType::Float64),
            Field::new("bhcap857".into(), DataType::Float64),
            Field::new("bhcap858".into(), DataType::Float64),
            Field::new("bhcap859".into(), DataType::Float64),
            Field::new("bhcap860".into(), DataType::Float64),
            Field::new("bhcap861".into(), DataType::Float64),
            Field::new("bhcap862".into(), DataType::Float64),
            Field::new("bhcap863".into(), DataType::Float64),
            Field::new("bhcap864".into(), DataType::Float64),
            Field::new("bhcap865".into(), DataType::Float64),
            Field::new("bhcap866".into(), DataType::Float64),
            Field::new("bhcap867".into(), DataType::Float64),
            Field::new("bhcap868".into(), DataType::Float64),
            Field::new("bhcap870".into(), DataType::Float64),
            Field::new("bhcap872".into(), DataType::Float64),
            Field::new("bhcap875".into(), DataType::Float64),
            Field::new("bhcaq257".into(), DataType::Float64),
            Field::new("bhcaq258".into(), DataType::Float64),
            Field::new("bhcas540".into(), DataType::Float64),
            Field::new("bhcb2210".into(), DataType::Float64),
            Field::new("bhcb2389".into(), DataType::Float64),
            Field::new("bhcb2604".into(), DataType::Float64),
            Field::new("bhcb3187".into(), DataType::Float64),
            Field::new("bhcb6648".into(), DataType::Float64),
            Field::new("bhcbhk29".into(), DataType::Float64),
            Field::new("bhcbj474".into(), DataType::Float64),
            Field::new("bhce0010".into(), DataType::Float64),
            Field::new("bhce1727".into(), DataType::Float64),
            Field::new("bhce1754".into(), DataType::Float64),
            Field::new("bhce1773".into(), DataType::Float64),
            Field::new("bhce2170".into(), DataType::Float64),
            Field::new("bhce3123".into(), DataType::Float64),
            Field::new("bhce3411".into(), DataType::Float64),
            Field::new("bhce3429".into(), DataType::Float64),
            Field::new("bhce3433".into(), DataType::Float64),
            Field::new("bhce3545".into(), DataType::Float64),
            Field::new("bhce5369".into(), DataType::Float64),
            Field::new("bhce6566".into(), DataType::Float64),
            Field::new("bhce6570".into(), DataType::Float64),
            Field::new("bhce6572".into(), DataType::Float64),
            Field::new("bhcea167".into(), DataType::Float64),
            Field::new("bhcea250".into(), DataType::Float64),
            Field::new("bhceb528".into(), DataType::Float64),
            Field::new("bhceb541".into(), DataType::Float64),
            Field::new("bhceb546".into(), DataType::Float64),
            Field::new("bhceb639".into(), DataType::Float64),
            Field::new("bhceb675".into(), DataType::Float64),
            Field::new("bhceb681".into(), DataType::Float64),
            Field::new("bhceg591".into(), DataType::Float64),
            Field::new("bhcm3531".into(), DataType::Float64),
            Field::new("bhcm3532".into(), DataType::Float64),
            Field::new("bhcm3533".into(), DataType::Float64),
            Field::new("bhcm3534".into(), DataType::Float64),
            Field::new("bhcm3535".into(), DataType::Float64),
            Field::new("bhcm3536".into(), DataType::Float64),
            Field::new("bhcm3537".into(), DataType::Float64),
            Field::new("bhcm3541".into(), DataType::Float64),
            Field::new("bhcm3543".into(), DataType::Float64),
            Field::new("bhcp0010".into(), DataType::Float64),
            Field::new("bhcp0087".into(), DataType::Float64),
            Field::new("bhcp0201".into(), DataType::Float64),
            Field::new("bhcp0202".into(), DataType::Float64),
            Field::new("bhcp0203".into(), DataType::Float64),
            Field::new("bhcp0204".into(), DataType::Float64),
            Field::new("bhcp0205".into(), DataType::Float64),
            Field::new("bhcp0206".into(), DataType::Float64),
            Field::new("bhcp0207".into(), DataType::Float64),
            Field::new("bhcp0208".into(), DataType::Float64),
            Field::new("bhcp0209".into(), DataType::Float64),
            Field::new("bhcp0210".into(), DataType::Float64),
            Field::new("bhcp0277".into(), DataType::Float64),
            Field::new("bhcp0279".into(), DataType::Float64),
            Field::new("bhcp0362".into(), DataType::Float64),
            Field::new("bhcp0363".into(), DataType::Float64),
            Field::new("bhcp0364".into(), DataType::Float64),
            Field::new("bhcp0365".into(), DataType::Float64),
            Field::new("bhcp0368".into(), DataType::Float64),
            Field::new("bhcp0400".into(), DataType::Float64),
            Field::new("bhcp0416".into(), DataType::Float64),
            Field::new("bhcp0447".into(), DataType::Float64),
            Field::new("bhcp0467".into(), DataType::Float64),
            Field::new("bhcp0496".into(), DataType::Float64),
            Field::new("bhcp0508".into(), DataType::Float64),
            Field::new("bhcp0512".into(), DataType::Float64),
            Field::new("bhcp0515".into(), DataType::Float64),
            Field::new("bhcp0518".into(), DataType::Float64),
            Field::new("bhcp0520".into(), DataType::Float64),
            Field::new("bhcp0522".into(), DataType::Float64),
            Field::new("bhcp0533".into(), DataType::Float64),
            Field::new("bhcp0534".into(), DataType::Float64),
            Field::new("bhcp0536".into(), DataType::Float64),
            Field::new("bhcp0537".into(), DataType::Float64),
            Field::new("bhcp0538".into(), DataType::Float64),
            Field::new("bhcp0539".into(), DataType::Float64),
            Field::new("bhcp0540".into(), DataType::Float64),
            Field::new("bhcp0541".into(), DataType::Float64),
            Field::new("bhcp0542".into(), DataType::Float64),
            Field::new("bhcp0543".into(), DataType::Float64),
            Field::new("bhcp1273".into(), DataType::Float64),
            Field::new("bhcp1274".into(), DataType::Float64),
            Field::new("bhcp1275".into(), DataType::Float64),
            Field::new("bhcp1276".into(), DataType::Float64),
            Field::new("bhcp1277".into(), DataType::Float64),
            Field::new("bhcp1278".into(), DataType::Float64),
            Field::new("bhcp1279".into(), DataType::Float64),
            Field::new("bhcp1299".into(), DataType::Float64),
            Field::new("bhcp1403".into(), DataType::Float64),
            Field::new("bhcp1407".into(), DataType::Float64),
            Field::new("bhcp1616".into(), DataType::Float64),
            Field::new("bhcp2123".into(), DataType::Float64),
            Field::new("bhcp2125".into(), DataType::Float64),
            Field::new("bhcp2145".into(), DataType::Float64),
            Field::new("bhcp2160".into(), DataType::Float64),
            Field::new("bhcp2165".into(), DataType::Float64),
            Field::new("bhcp2170".into(), DataType::Float64),
            Field::new("bhcp2200".into(), DataType::Float64),
            Field::new("bhcp2309".into(), DataType::Float64),
            Field::new("bhcp2332".into(), DataType::Float64),
            Field::new("bhcp2792".into(), DataType::Float64),
            Field::new("bhcp2793".into(), DataType::Float64),
            Field::new("bhcp2794".into(), DataType::Float64),
            Field::new("bhcp2796".into(), DataType::Float64),
            Field::new("bhcp2831".into(), DataType::Float64),
            Field::new("bhcp2930".into(), DataType::Float64),
            Field::new("bhcp3123".into(), DataType::Float64),
            Field::new("bhcp3128".into(), DataType::Float64),
            Field::new("bhcp3147".into(), DataType::Float64),
            Field::new("bhcp3152".into(), DataType::Float64),
            Field::new("bhcp3153".into(), DataType::Float64),
            Field::new("bhcp3156".into(), DataType::Float64),
            Field::new("bhcp3163".into(), DataType::Float64),
            Field::new("bhcp3164".into(), DataType::Float64),
            Field::new("bhcp3165".into(), DataType::Float64),
            Field::new("bhcp3210".into(), DataType::Float64),
            Field::new("bhcp3230".into(), DataType::Float64),
            Field::new("bhcp3238".into(), DataType::Float64),
            Field::new("bhcp3239".into(), DataType::Float64),
            Field::new("bhcp3240".into(), DataType::Float64),
            Field::new("bhcp3247".into(), DataType::Float64),
            Field::new("bhcp3283".into(), DataType::Float64),
            Field::new("bhcp3290".into(), DataType::Float64),
            Field::new("bhcp3293".into(), DataType::Float64),
            Field::new("bhcp3298".into(), DataType::Float64),
            Field::new("bhcp3300".into(), DataType::Float64),
            Field::new("bhcp3409".into(), DataType::Float64),
            Field::new("bhcp3513".into(), DataType::Float64),
            Field::new("bhcp3602".into(), DataType::Float64),
            Field::new("bhcp3603".into(), DataType::Float64),
            Field::new("bhcp3604".into(), DataType::Float64),
            Field::new("bhcp3605".into(), DataType::Float64),
            Field::new("bhcp3606".into(), DataType::Float64),
            Field::new("bhcp3607".into(), DataType::Float64),
            Field::new("bhcp3609".into(), DataType::Float64),
            Field::new("bhcp3611".into(), DataType::Float64),
            Field::new("bhcp3612".into(), DataType::Float64),
            Field::new("bhcp3613".into(), DataType::Float64),
            Field::new("bhcp3614".into(), DataType::Float64),
            Field::new("bhcp3615".into(), DataType::Float64),
            Field::new("bhcp3616".into(), DataType::Float64),
            Field::new("bhcp3617".into(), DataType::Float64),
            Field::new("bhcp3618".into(), DataType::Float64),
            Field::new("bhcp3619".into(), DataType::Float64),
            Field::new("bhcp4000".into(), DataType::Float64),
            Field::new("bhcp4062".into(), DataType::Float64),
            Field::new("bhcp4073".into(), DataType::Float64),
            Field::new("bhcp4091".into(), DataType::Float64),
            Field::new("bhcp4130".into(), DataType::Float64),
            Field::new("bhcp4135".into(), DataType::Float64),
            Field::new("bhcp4230".into(), DataType::Float64),
            Field::new("bhcp4243".into(), DataType::Float64),
            Field::new("bhcp4250".into(), DataType::Float64),
            Field::new("bhcp4302".into(), DataType::Float64),
            Field::new("bhcp4320".into(), DataType::Float64),
            Field::new("bhcp4336".into(), DataType::Float64),
            Field::new("bhcp4340".into(), DataType::Float64),
            Field::new("bhcp4485".into(), DataType::Float64),
            Field::new("bhcp4605".into(), DataType::Float64),
            Field::new("bhcp4635".into(), DataType::Float64),
            Field::new("bhcp4647".into(), DataType::Float64),
            Field::new("bhcp4778".into(), DataType::Float64),
            Field::new("bhcp5485".into(), DataType::Float64),
            Field::new("bhcp5486".into(), DataType::Float64),
            Field::new("bhcp5487".into(), DataType::Float64),
            Field::new("bhcp5488".into(), DataType::Float64),
            Field::new("bhcp5489".into(), DataType::Float64),
            Field::new("bhcp5993".into(), DataType::Float64),
            Field::new("bhcp6552".into(), DataType::Float64),
            Field::new("bhcp6567".into(), DataType::Float64),
            Field::new("bhcp6571".into(), DataType::Float64),
            Field::new("bhcp6573".into(), DataType::Float64),
            Field::new("bhcp6588".into(), DataType::Float64),
            Field::new("bhcp6589".into(), DataType::Float64),
            Field::new("bhcp6590".into(), DataType::Float64),
            Field::new("bhcp6591".into(), DataType::Float64),
            Field::new("bhcp6592".into(), DataType::Float64),
            Field::new("bhcp6596".into(), DataType::Float64),
            Field::new("bhcp6600".into(), DataType::Float64),
            Field::new("bhcp6604".into(), DataType::Float64),
            Field::new("bhcp6607".into(), DataType::Float64),
            Field::new("bhcp6619".into(), DataType::Float64),
            Field::new("bhcp6649".into(), DataType::Float64),
            Field::new("bhcp6741".into(), DataType::Float64),
            Field::new("bhcp6742".into(), DataType::Float64),
            Field::new("bhcp6743".into(), DataType::Float64),
            Field::new("bhcp6744".into(), DataType::Float64),
            Field::new("bhcp6758".into(), DataType::Float64),
            Field::new("bhcp6773".into(), DataType::Float64),
            Field::new("bhcp6775".into(), DataType::Float64),
            Field::new("bhcp6791".into(), DataType::Float64),
            Field::new("bhcp6792".into(), DataType::Float64),
            Field::new("bhcp6793".into(), DataType::Float64),
            Field::new("bhcp6794".into(), DataType::Float64),
            Field::new("bhcp6795".into(), DataType::Float64),
            Field::new("bhcp8434".into(), DataType::Float64),
            Field::new("bhcp8516".into(), DataType::Float64),
            Field::new("bhcp8517".into(), DataType::Float64),
            Field::new("bhcp8518".into(), DataType::Float64),
            Field::new("bhcp8843".into(), DataType::Float64),
            Field::new("bhcp9191".into(), DataType::Float64),
            Field::new("bhcp9802".into(), DataType::Float64),
            Field::new("bhcpa130".into(), DataType::Float64),
            Field::new("bhcpb530".into(), DataType::Float64),
            Field::new("bhcpc254".into(), DataType::Float64),
            Field::new("bhcpc255".into(), DataType::Float64),
            Field::new("bhcpc427".into(), DataType::Float64),
            Field::new("bhcpc428".into(), DataType::Float64),
            Field::new("bhcpc447".into(), DataType::Float64),
            Field::new("bhcpf229".into(), DataType::Float64),
            Field::new("bhcpf737".into(), DataType::Float64),
            Field::new("bhcpf817".into(), DataType::Float64),
            Field::new("bhcpf818".into(), DataType::Float64),
            Field::new("bhcpf819".into(), DataType::Float64),
            Field::new("bhcpf820".into(), DataType::Float64),
            Field::new("bhcpf838".into(), DataType::Float64),
            Field::new("bhcpf841".into(), DataType::Boolean),
            Field::new("bhcpf842".into(), DataType::Boolean),
            Field::new("bhcpft28".into(), DataType::Float64),
            Field::new("bhcphk02".into(), DataType::Float64),
            Field::new("bhcpht69".into(), DataType::Float64),
            Field::new("bhcpht70".into(), DataType::Float64),
            Field::new("bhcphu25".into(), DataType::Float64),
            Field::new("bhcphu26".into(), DataType::Float64),
            Field::new("bhcpj980".into(), DataType::Float64),
            Field::new("bhcpja22".into(), DataType::Float64),
            Field::new("bhcpjj33".into(), DataType::Float64),
            Field::new("bhcpk297".into(), DataType::Float64),
            Field::new("bhcpky38".into(), DataType::Boolean),
            Field::new("bhcpm962".into(), DataType::Float64),
            Field::new("bhct0426".into(), DataType::Float64),
            Field::new("bhct1754".into(), DataType::Float64),
            Field::new("bhct1773".into(), DataType::Float64),
            Field::new("bhct2143".into(), DataType::Float64),
            Field::new("bhct2150".into(), DataType::Float64),
            Field::new("bhct2160".into(), DataType::Float64),
            Field::new("bhct2170".into(), DataType::Float64),
            Field::new("bhct2750".into(), DataType::Float64),
            Field::new("bhct3123".into(), DataType::Float64),
            Field::new("bhct3190".into(), DataType::Float64),
            Field::new("bhct3210".into(), DataType::Float64),
            Field::new("bhct3247".into(), DataType::Float64),
            Field::new("bhct3368".into(), DataType::Float64),
            Field::new("bhct3411".into(), DataType::Float64),
            Field::new("bhct3433".into(), DataType::Float64),
            Field::new("bhct3543".into(), DataType::Float64),
            Field::new("bhct3545".into(), DataType::Float64),
            Field::new("bhct3547".into(), DataType::Float64),
            Field::new("bhct3548".into(), DataType::Float64),
            Field::new("bhct4230".into(), DataType::Float64),
            Field::new("bhct4340".into(), DataType::Float64),
            Field::new("bhct4605".into(), DataType::Float64),
            Field::new("bhct5369".into(), DataType::Float64),
            Field::new("bhct5610".into(), DataType::Float64),
            Field::new("bhct6570".into(), DataType::Float64),
            Field::new("bhcta250".into(), DataType::Float64),
            Field::new("bhctb528".into(), DataType::Float64),
            Field::new("bhctb590".into(), DataType::Float64),
            Field::new("bhctb591".into(), DataType::Float64),
            Field::new("bhcw3792".into(), DataType::Float64),
            Field::new("bhcw5310".into(), DataType::Float64),
            Field::new("bhcw5311".into(), DataType::Float64),
            Field::new("bhcw7205".into(), DataType::Float64),
            Field::new("bhcw7206".into(), DataType::Float64),
            Field::new("bhcwa223".into(), DataType::Float64),
            Field::new("bhcwh311".into(), DataType::Float64),
            Field::new("bhcwkx78".into(), DataType::Boolean),
            Field::new("bhcwkx83".into(), DataType::Boolean),
            Field::new("bhcwle85".into(), DataType::Float64),
            Field::new("bhcwle86".into(), DataType::Float64),
            Field::new("bhcwle87".into(), DataType::Float64),
            Field::new("bhcwlf23".into(), DataType::Float64),
            Field::new("bhcwlf24".into(), DataType::Float64),
            Field::new("bhcwlf25".into(), DataType::Float64),
            Field::new("bhcwmk66".into(), DataType::Float64),
            Field::new("bhcwp793".into(), DataType::Float64),
            Field::new("bhcwp851".into(), DataType::Float64),
            Field::new("bhcwp852".into(), DataType::Float64),
            Field::new("bhcwp853".into(), DataType::Float64),
            Field::new("bhcwp854".into(), DataType::Float64),
            Field::new("bhcwp855".into(), DataType::Float64),
            Field::new("bhcwp856".into(), DataType::Float64),
            Field::new("bhcwp857".into(), DataType::Float64),
            Field::new("bhcwp858".into(), DataType::Float64),
            Field::new("bhcwp859".into(), DataType::Float64),
            Field::new("bhcwp870".into(), DataType::Float64),
            Field::new("bhcx1754".into(), DataType::Float64),
            Field::new("bhcx1773".into(), DataType::Float64),
            Field::new("bhcx3123".into(), DataType::Float64),
            Field::new("bhcx3210".into(), DataType::Float64),
            Field::new("bhcx3368".into(), DataType::Float64),
            Field::new("bhcx3545".into(), DataType::Float64),
            Field::new("bhcy1773".into(), DataType::Float64),
            Field::new("bhcy3123".into(), DataType::Float64),
            Field::new("bhcyja36".into(), DataType::Float64),
            Field::new("bhdm1288".into(), DataType::Float64),
            Field::new("bhdm1410".into(), DataType::Float64),
            Field::new("bhdm1415".into(), DataType::Float64),
            Field::new("bhdm1420".into(), DataType::Float64),
            Field::new("bhdm1460".into(), DataType::Float64),
            Field::new("bhdm1480".into(), DataType::Float64),
            Field::new("bhdm1545".into(), DataType::Float64),
            Field::new("bhdm1564".into(), DataType::Float64),
            Field::new("bhdm1590".into(), DataType::Float64),
            Field::new("bhdm1635".into(), DataType::Float64),
            Field::new("bhdm1755".into(), DataType::Float64),
            Field::new("bhdm1766".into(), DataType::Float64),
            Field::new("bhdm1797".into(), DataType::Float64),
            Field::new("bhdm1975".into(), DataType::Float64),
            Field::new("bhdm2081".into(), DataType::Float64),
            Field::new("bhdm2122".into(), DataType::Float64),
            Field::new("bhdm2123".into(), DataType::Float64),
            Field::new("bhdm2165".into(), DataType::Float64),
            Field::new("bhdm3386".into(), DataType::Float64),
            Field::new("bhdm3387".into(), DataType::Float64),
            Field::new("bhdm3465".into(), DataType::Float64),
            Field::new("bhdm3466".into(), DataType::Float64),
            Field::new("bhdm3516".into(), DataType::Float64),
            Field::new("bhdm3545".into(), DataType::Float64),
            Field::new("bhdm3546".into(), DataType::Float64),
            Field::new("bhdm3547".into(), DataType::Float64),
            Field::new("bhdm3548".into(), DataType::Float64),
            Field::new("bhdm5367".into(), DataType::Float64),
            Field::new("bhdm5368".into(), DataType::Float64),
            Field::new("bhdm6631".into(), DataType::Float64),
            Field::new("bhdm6636".into(), DataType::Float64),
            Field::new("bhdma164".into(), DataType::Float64),
            Field::new("bhdma242".into(), DataType::Float64),
            Field::new("bhdma243".into(), DataType::Float64),
            Field::new("bhdmb561".into(), DataType::Float64),
            Field::new("bhdmb562".into(), DataType::Float64),
            Field::new("bhdmb987".into(), DataType::Float64),
            Field::new("bhdmb993".into(), DataType::Float64),
            Field::new("bhdmf560".into(), DataType::Float64),
            Field::new("bhdmf576".into(), DataType::Float64),
            Field::new("bhdmf577".into(), DataType::Float64),
            Field::new("bhdmf578".into(), DataType::Float64),
            Field::new("bhdmf579".into(), DataType::Float64),
            Field::new("bhdmf580".into(), DataType::Float64),
            Field::new("bhdmf581".into(), DataType::Float64),
            Field::new("bhdmf582".into(), DataType::Float64),
            Field::new("bhdmf583".into(), DataType::Float64),
            Field::new("bhdmf584".into(), DataType::Float64),
            Field::new("bhdmf585".into(), DataType::Float64),
            Field::new("bhdmf586".into(), DataType::Float64),
            Field::new("bhdmf587".into(), DataType::Float64),
            Field::new("bhdmf588".into(), DataType::Float64),
            Field::new("bhdmf589".into(), DataType::Float64),
            Field::new("bhdmf590".into(), DataType::Float64),
            Field::new("bhdmf591".into(), DataType::Float64),
            Field::new("bhdmf592".into(), DataType::Float64),
            Field::new("bhdmf593".into(), DataType::Float64),
            Field::new("bhdmf594".into(), DataType::Float64),
            Field::new("bhdmf595".into(), DataType::Float64),
            Field::new("bhdmf596".into(), DataType::Float64),
            Field::new("bhdmf597".into(), DataType::Float64),
            Field::new("bhdmf598".into(), DataType::Float64),
            Field::new("bhdmf599".into(), DataType::Float64),
            Field::new("bhdmf600".into(), DataType::Float64),
            Field::new("bhdmf601".into(), DataType::Float64),
            Field::new("bhdmf604".into(), DataType::Float64),
            Field::new("bhdmf605".into(), DataType::Float64),
            Field::new("bhdmf606".into(), DataType::Float64),
            Field::new("bhdmf607".into(), DataType::Float64),
            Field::new("bhdmf611".into(), DataType::Float64),
            Field::new("bhdmf612".into(), DataType::Float64),
            Field::new("bhdmf613".into(), DataType::Float64),
            Field::new("bhdmf614".into(), DataType::Float64),
            Field::new("bhdmf615".into(), DataType::Float64),
            Field::new("bhdmf616".into(), DataType::Float64),
            Field::new("bhdmf617".into(), DataType::Float64),
            Field::new("bhdmf618".into(), DataType::Float64),
            Field::new("bhdmf624".into(), DataType::Float64),
            Field::new("bhdmf625".into(), DataType::Float64),
            Field::new("bhdmf626".into(), DataType::Float64),
            Field::new("bhdmf627".into(), DataType::Float64),
            Field::new("bhdmf628".into(), DataType::Float64),
            Field::new("bhdmf629".into(), DataType::Float64),
            Field::new("bhdmf630".into(), DataType::Float64),
            Field::new("bhdmf631".into(), DataType::Float64),
            Field::new("bhdmf632".into(), DataType::Float64),
            Field::new("bhdmf633".into(), DataType::Float64),
            Field::new("bhdmf634".into(), DataType::Float64),
            Field::new("bhdmf635".into(), DataType::Float64),
            Field::new("bhdmf636".into(), DataType::Float64),
            Field::new("bhdmf639".into(), DataType::Float64),
            Field::new("bhdmf640".into(), DataType::Float64),
            Field::new("bhdmf670".into(), DataType::Float64),
            Field::new("bhdmf671".into(), DataType::Float64),
            Field::new("bhdmf672".into(), DataType::Float64),
            Field::new("bhdmf673".into(), DataType::Float64),
            Field::new("bhdmf674".into(), DataType::Float64),
            Field::new("bhdmf675".into(), DataType::Float64),
            Field::new("bhdmf676".into(), DataType::Float64),
            Field::new("bhdmf677".into(), DataType::Float64),
            Field::new("bhdmf678".into(), DataType::Float64),
            Field::new("bhdmf679".into(), DataType::Float64),
            Field::new("bhdmf680".into(), DataType::Float64),
            Field::new("bhdmf681".into(), DataType::Float64),
            Field::new("bhdmf724".into(), DataType::Float64),
            Field::new("bhdmg209".into(), DataType::Float64),
            Field::new("bhdmg210".into(), DataType::Float64),
            Field::new("bhdmg211".into(), DataType::Float64),
            Field::new("bhdmg299".into(), DataType::Float64),
            Field::new("bhdmg332".into(), DataType::Float64),
            Field::new("bhdmg333".into(), DataType::Float64),
            Field::new("bhdmg334".into(), DataType::Float64),
            Field::new("bhdmg335".into(), DataType::Float64),
            Field::new("bhdmg379".into(), DataType::Float64),
            Field::new("bhdmg380".into(), DataType::Float64),
            Field::new("bhdmg381".into(), DataType::Float64),
            Field::new("bhdmg382".into(), DataType::Float64),
            Field::new("bhdmg383".into(), DataType::Float64),
            Field::new("bhdmg384".into(), DataType::Float64),
            Field::new("bhdmg385".into(), DataType::Float64),
            Field::new("bhdmg386".into(), DataType::Float64),
            Field::new("bhdmg387".into(), DataType::Float64),
            Field::new("bhdmg388".into(), DataType::Float64),
            Field::new("bhdmg651".into(), DataType::Float64),
            Field::new("bhdmg652".into(), DataType::Float64),
            Field::new("bhdmhk06".into(), DataType::Float64),
            Field::new("bhdmhk31".into(), DataType::Float64),
            Field::new("bhdmhk32".into(), DataType::Float64),
            Field::new("bhdmj451".into(), DataType::Float64),
            Field::new("bhdmj454".into(), DataType::Float64),
            Field::new("bhdmk045".into(), DataType::Float64),
            Field::new("bhdmk046".into(), DataType::Float64),
            Field::new("bhdmk047".into(), DataType::Float64),
            Field::new("bhdmk048".into(), DataType::Float64),
            Field::new("bhdmk049".into(), DataType::Float64),
            Field::new("bhdmk050".into(), DataType::Float64),
            Field::new("bhdmk051".into(), DataType::Float64),
            Field::new("bhdmk052".into(), DataType::Float64),
            Field::new("bhdmk053".into(), DataType::Float64),
            Field::new("bhdmk054".into(), DataType::Float64),
            Field::new("bhdmk055".into(), DataType::Float64),
            Field::new("bhdmk056".into(), DataType::Float64),
            Field::new("bhdmk057".into(), DataType::Float64),
            Field::new("bhdmk058".into(), DataType::Float64),
            Field::new("bhdmk059".into(), DataType::Float64),
            Field::new("bhdmk060".into(), DataType::Float64),
            Field::new("bhdmk061".into(), DataType::Float64),
            Field::new("bhdmk062".into(), DataType::Float64),
            Field::new("bhdmk063".into(), DataType::Float64),
            Field::new("bhdmk064".into(), DataType::Float64),
            Field::new("bhdmk065".into(), DataType::Float64),
            Field::new("bhdmk066".into(), DataType::Float64),
            Field::new("bhdmk067".into(), DataType::Float64),
            Field::new("bhdmk068".into(), DataType::Float64),
            Field::new("bhdmk069".into(), DataType::Float64),
            Field::new("bhdmk070".into(), DataType::Float64),
            Field::new("bhdmk071".into(), DataType::Float64),
            Field::new("bhdmk105".into(), DataType::Float64),
            Field::new("bhdmk106".into(), DataType::Float64),
            Field::new("bhdmk107".into(), DataType::Float64),
            Field::new("bhdmk108".into(), DataType::Float64),
            Field::new("bhdmk109".into(), DataType::Float64),
            Field::new("bhdmk110".into(), DataType::Float64),
            Field::new("bhdmk111".into(), DataType::Float64),
            Field::new("bhdmk112".into(), DataType::Float64),
            Field::new("bhdmk113".into(), DataType::Float64),
            Field::new("bhdmk114".into(), DataType::Float64),
            Field::new("bhdmk115".into(), DataType::Float64),
            Field::new("bhdmk116".into(), DataType::Float64),
            Field::new("bhdmk117".into(), DataType::Float64),
            Field::new("bhdmk118".into(), DataType::Float64),
            Field::new("bhdmk119".into(), DataType::Float64),
            Field::new("bhdmk130".into(), DataType::Float64),
            Field::new("bhdmk131".into(), DataType::Float64),
            Field::new("bhdmk132".into(), DataType::Float64),
            Field::new("bhdmk158".into(), DataType::Float64),
            Field::new("bhdmk159".into(), DataType::Float64),
            Field::new("bhdmk160".into(), DataType::Float64),
            Field::new("bhdmk161".into(), DataType::Float64),
            Field::new("bhdmk162".into(), DataType::Float64),
            Field::new("bhdmk166".into(), DataType::Float64),
            Field::new("bhdmk169".into(), DataType::Float64),
            Field::new("bhdmk170".into(), DataType::Float64),
            Field::new("bhdmk171".into(), DataType::Float64),
            Field::new("bhdmk172".into(), DataType::Float64),
            Field::new("bhdmk173".into(), DataType::Float64),
            Field::new("bhdmk174".into(), DataType::Float64),
            Field::new("bhdmk175".into(), DataType::Float64),
            Field::new("bhdmk176".into(), DataType::Float64),
            Field::new("bhdmk177".into(), DataType::Float64),
            Field::new("bhdmk187".into(), DataType::Float64),
            Field::new("bhdmk188".into(), DataType::Float64),
            Field::new("bhdmk189".into(), DataType::Float64),
            Field::new("bhdmk190".into(), DataType::Float64),
            Field::new("bhdmk191".into(), DataType::Float64),
            Field::new("bhdmk195".into(), DataType::Float64),
            Field::new("bhdmk196".into(), DataType::Float64),
            Field::new("bhdmk197".into(), DataType::Float64),
            Field::new("bhdmk198".into(), DataType::Float64),
            Field::new("bhdmk199".into(), DataType::Float64),
            Field::new("bhdmk200".into(), DataType::Float64),
            Field::new("bhdmk208".into(), DataType::Float64),
            Field::new("bhdmk209".into(), DataType::Float64),
            Field::new("bhdmk210".into(), DataType::Float64),
            Field::new("bhdmk211".into(), DataType::Float64),
            Field::new("bhdmkx57".into(), DataType::Float64),
            Field::new("bhfn3360".into(), DataType::Float64),
            Field::new("bhfn3543".into(), DataType::Float64),
            Field::new("bhfn6631".into(), DataType::Float64),
            Field::new("bhfn6636".into(), DataType::Float64),
            Field::new("bhfna245".into(), DataType::Float64),
            Field::new("bhfnk260".into(), DataType::Float64),
            Field::new("bhod2389".into(), DataType::Float64),
            Field::new("bhod2604".into(), DataType::Float64),
            Field::new("bhod3187".into(), DataType::Float64),
            Field::new("bhod3189".into(), DataType::Float64),
            Field::new("bhod6648".into(), DataType::Float64),
            Field::new("bhodhk29".into(), DataType::Float64),
            Field::new("bhodj474".into(), DataType::Float64),
            Field::new("bhpa0365".into(), DataType::Float64),
            Field::new("bhpa4340".into(), DataType::Float64),
            Field::new("bhpx8901".into(), DataType::String),
            Field::new("bhsp0010".into(), DataType::Float64),
            Field::new("bhsp0027".into(), DataType::Float64),
            Field::new("bhsp0087".into(), DataType::Float64),
            Field::new("bhsp0088".into(), DataType::Float64),
            Field::new("bhsp0089".into(), DataType::Float64),
            Field::new("bhsp0201".into(), DataType::Float64),
            Field::new("bhsp0202".into(), DataType::Float64),
            Field::new("bhsp0206".into(), DataType::Float64),
            Field::new("bhsp0390".into(), DataType::Float64),
            Field::new("bhsp0416".into(), DataType::Float64),
            Field::new("bhsp0447".into(), DataType::Float64),
            Field::new("bhsp0496".into(), DataType::Float64),
            Field::new("bhsp0508".into(), DataType::Float64),
            Field::new("bhsp0523".into(), DataType::Float64),
            Field::new("bhsp0530".into(), DataType::Float64),
            Field::new("bhsp1283".into(), DataType::Float64),
            Field::new("bhsp2111".into(), DataType::Float64),
            Field::new("bhsp2112".into(), DataType::Float64),
            Field::new("bhsp2122".into(), DataType::Float64),
            Field::new("bhsp2145".into(), DataType::Float64),
            Field::new("bhsp2148".into(), DataType::Float64),
            Field::new("bhsp2170".into(), DataType::Float64),
            Field::new("bhsp2309".into(), DataType::Float64),
            Field::new("bhsp2723".into(), DataType::Float64),
            Field::new("bhsp2724".into(), DataType::Float64),
            Field::new("bhsp2792".into(), DataType::Float64),
            Field::new("bhsp2794".into(), DataType::Float64),
            Field::new("bhsp2796".into(), DataType::Float64),
            Field::new("bhsp2932".into(), DataType::Float64),
            Field::new("bhsp3049".into(), DataType::Float64),
            Field::new("bhsp3066".into(), DataType::Float64),
            Field::new("bhsp3123".into(), DataType::Float64),
            Field::new("bhsp3148".into(), DataType::Float64),
            Field::new("bhsp3151".into(), DataType::Float64),
            Field::new("bhsp3152".into(), DataType::Float64),
            Field::new("bhsp3153".into(), DataType::Float64),
            Field::new("bhsp3154".into(), DataType::Float64),
            Field::new("bhsp3155".into(), DataType::Float64),
            Field::new("bhsp3156".into(), DataType::Float64),
            Field::new("bhsp3158".into(), DataType::Float64),
            Field::new("bhsp3166".into(), DataType::Float64),
            Field::new("bhsp3167".into(), DataType::Float64),
            Field::new("bhsp3210".into(), DataType::Float64),
            Field::new("bhsp3230".into(), DataType::Float64),
            Field::new("bhsp3238".into(), DataType::Float64),
            Field::new("bhsp3239".into(), DataType::Float64),
            Field::new("bhsp3247".into(), DataType::Float64),
            Field::new("bhsp3283".into(), DataType::Float64),
            Field::new("bhsp3300".into(), DataType::Float64),
            Field::new("bhsp3513".into(), DataType::Float64),
            Field::new("bhsp3523".into(), DataType::Float64),
            Field::new("bhsp3524".into(), DataType::Float64),
            Field::new("bhsp3525".into(), DataType::Float64),
            Field::new("bhsp3526".into(), DataType::Float64),
            Field::new("bhsp3527".into(), DataType::Float64),
            Field::new("bhsp3605".into(), DataType::Float64),
            Field::new("bhsp3620".into(), DataType::Float64),
            Field::new("bhsp3621".into(), DataType::Float64),
            Field::new("bhsp4000".into(), DataType::Float64),
            Field::new("bhsp4073".into(), DataType::Float64),
            Field::new("bhsp4093".into(), DataType::Float64),
            Field::new("bhsp4130".into(), DataType::Float64),
            Field::new("bhsp4250".into(), DataType::Float64),
            Field::new("bhsp4302".into(), DataType::Float64),
            Field::new("bhsp4336".into(), DataType::Float64),
            Field::new("bhsp4340".into(), DataType::Float64),
            Field::new("bhsp4778".into(), DataType::Float64),
            Field::new("bhsp5993".into(), DataType::Float64),
            Field::new("bhsp6416".into(), DataType::Float64),
            Field::new("bhsp6649".into(), DataType::Float64),
            Field::new("bhsp6796".into(), DataType::Float64),
            Field::new("bhsp6797".into(), DataType::Float64),
            Field::new("bhsp8434".into(), DataType::Float64),
            Field::new("bhsp8516".into(), DataType::Float64),
            Field::new("bhsp8517".into(), DataType::Float64),
            Field::new("bhsp8519".into(), DataType::Float64),
            Field::new("bhsp8520".into(), DataType::Float64),
            Field::new("bhsp8521".into(), DataType::Float64),
            Field::new("bhsp8522".into(), DataType::Float64),
            Field::new("bhsp8523".into(), DataType::Float64),
            Field::new("bhsp8524".into(), DataType::Float64),
            Field::new("bhsp8525".into(), DataType::Float64),
            Field::new("bhsp8526".into(), DataType::Float64),
            Field::new("bhsp8527".into(), DataType::Float64),
            Field::new("bhsp8528".into(), DataType::Float64),
            Field::new("bhsp8529".into(), DataType::Float64),
            Field::new("bhsp8530".into(), DataType::Float64),
            Field::new("bhsp8843".into(), DataType::Float64),
            Field::new("bhsp9191".into(), DataType::Float64),
            Field::new("bhsp9802".into(), DataType::Float64),
            Field::new("bhspa024".into(), DataType::Float64),
            Field::new("bhspa130".into(), DataType::Float64),
            Field::new("bhspa530".into(), DataType::Float64),
            Field::new("bhspb530".into(), DataType::Float64),
            Field::new("bhspc009".into(), DataType::Float64),
            Field::new("bhspc159".into(), DataType::Float64),
            Field::new("bhspc160".into(), DataType::Boolean),
            Field::new("bhspc161".into(), DataType::Float64),
            Field::new("bhspc252".into(), DataType::Float64),
            Field::new("bhspc253".into(), DataType::Float64),
            Field::new("bhspc254".into(), DataType::Float64),
            Field::new("bhspc255".into(), DataType::Float64),
            Field::new("bhspc256".into(), DataType::Float64),
            Field::new("bhspc257".into(), DataType::Float64),
            Field::new("bhspc427".into(), DataType::Float64),
            Field::new("bhspc428".into(), DataType::Float64),
            Field::new("bhspc447".into(), DataType::Float64),
            Field::new("bhspc700".into(), DataType::Float64),
            Field::new("bhspc701".into(), DataType::Float64),
            Field::new("bhspc702".into(), DataType::Float64),
            Field::new("bhspc884".into(), DataType::Float64),
            Field::new("bhspf074".into(), DataType::Float64),
            Field::new("bhspf075".into(), DataType::Float64),
            Field::new("bhspf229".into(), DataType::Float64),
            Field::new("bhspf819".into(), DataType::Float64),
            Field::new("bhspf820".into(), DataType::Float64),
            Field::new("bhspf838".into(), DataType::Float64),
            Field::new("bhspf841".into(), DataType::Boolean),
            Field::new("bhspf842".into(), DataType::Boolean),
            Field::new("bhspft28".into(), DataType::Float64),
            Field::new("bhspft42".into(), DataType::Boolean),
            Field::new("bhspft43".into(), DataType::Boolean),
            Field::new("bhspft44".into(), DataType::Boolean),
            Field::new("bhspg234".into(), DataType::Float64),
            Field::new("bhspg235".into(), DataType::Float64),
            Field::new("bhspht69".into(), DataType::Float64),
            Field::new("bhspht70".into(), DataType::Float64),
            Field::new("bhspht95".into(), DataType::Float64),
            Field::new("bhspj980".into(), DataType::Float64),
            Field::new("bhspk141".into(), DataType::Float64),
            Field::new("bhspky38".into(), DataType::Boolean),
            Field::new("bhspm962".into(), DataType::Float64),
            Field::new("bhspmz36".into(), DataType::Float64),
            Field::new("bhspnk60".into(), DataType::Boolean),
            Field::new("bhsx8901".into(), DataType::String),
            Field::new("bhtxf655".into(), DataType::String),
            Field::new("bhtxf656".into(), DataType::String),
            Field::new("bhtxf657".into(), DataType::String),
            Field::new("bhtxf658".into(), DataType::String),
            Field::new("bhtxf659".into(), DataType::String),
            Field::new("bhtxf660".into(), DataType::String),
            Field::new("bhtxg546".into(), DataType::String),
            Field::new("bhtxg551".into(), DataType::String),
            Field::new("bhtxg556".into(), DataType::String),
            Field::new("bhtxg561".into(), DataType::String),
            Field::new("bhtxg571".into(), DataType::String),
            Field::new("bhtxg576".into(), DataType::Boolean),
            Field::new("bhtxg581".into(), DataType::String),
            Field::new("bhtxg586".into(), DataType::String),
            Field::new("rssd4087".into(), DataType::String),
            Field::new("rssd6191".into(), DataType::Float64),
            Field::new("rssd9001".into(), DataType::Float64),
            Field::new("rssd9005".into(), DataType::String),
            Field::new("rssd9007".into(), DataType::Date),
            Field::new("rssd9008".into(), DataType::Date),
            Field::new("rssd9010".into(), DataType::String),
            Field::new("rssd9014".into(), DataType::Float64),
            Field::new("rssd9016".into(), DataType::Float64),
            Field::new("rssd9017".into(), DataType::String),
            Field::new("rssd9028".into(), DataType::String),
            Field::new("rssd9029".into(), DataType::String),
            Field::new("rssd9030".into(), DataType::Float64),
            Field::new("rssd9031".into(), DataType::Float64),
            Field::new("rssd9032".into(), DataType::Float64),
            Field::new("rssd9037".into(), DataType::Float64),
            Field::new("rssd9038".into(), DataType::String),
            Field::new("rssd9039".into(), DataType::Float64),
            Field::new("rssd9042".into(), DataType::Float64),
            Field::new("rssd9044".into(), DataType::Float64),
            Field::new("rssd9045".into(), DataType::Float64),
            Field::new("rssd9046".into(), DataType::Float64),
            Field::new("rssd9047".into(), DataType::Float64),
            Field::new("rssd9048".into(), DataType::Float64),
            Field::new("rssd9049".into(), DataType::Float64),
            Field::new("rssd9050".into(), DataType::Float64),
            Field::new("rssd9052".into(), DataType::Float64),
            Field::new("rssd9053".into(), DataType::Float64),
            Field::new("rssd9054".into(), DataType::Float64),
            Field::new("rssd9055".into(), DataType::Float64),
            Field::new("rssd9056".into(), DataType::Float64),
            Field::new("rssd9059".into(), DataType::Float64),
            Field::new("rssd9060".into(), DataType::Float64),
            Field::new("rssd9061".into(), DataType::Float64),
            Field::new("rssd9101".into(), DataType::String),
            Field::new("rssd9130".into(), DataType::String),
            Field::new("rssd9132".into(), DataType::Float64),
            Field::new("rssd9138".into(), DataType::Float64),
            Field::new("rssd9146".into(), DataType::Float64),
            Field::new("rssd9150".into(), DataType::Float64),
            Field::new("rssd9161".into(), DataType::String),
            Field::new("rssd9170".into(), DataType::Float64),
            Field::new("rssd9192".into(), DataType::String),
            Field::new("rssd9198".into(), DataType::Float64),
            Field::new("rssd9200".into(), DataType::String),
            Field::new("rssd9210".into(), DataType::Float64),
            Field::new("rssd9213".into(), DataType::Float64),
            Field::new("rssd9216".into(), DataType::Float64),
            Field::new("rssd9220".into(), DataType::String),
            Field::new("rssd9320".into(), DataType::Float64),
            Field::new("rssd9374".into(), DataType::Float64),
            Field::new("rssd9375".into(), DataType::Float64),
            Field::new("rssd9421".into(), DataType::Float64),
            Field::new("rssd9422".into(), DataType::Float64),
            Field::new("rssd9424".into(), DataType::Float64),
            Field::new("rssd9425".into(), DataType::Float64),
            Field::new("rssd9579".into(), DataType::Float64),
            Field::new("rssd9950".into(), DataType::Date),
            Field::new("rssd9955".into(), DataType::Float64),
            Field::new("rssd9999".into(), DataType::Float64), // DataType::Date),
            Field::new("texc3573".into(), DataType::Boolean),
            Field::new("texc3575".into(), DataType::Boolean),
            Field::new("texc6373".into(), DataType::Float64),
            Field::new("texc6561".into(), DataType::Boolean),
            Field::new("texc6562".into(), DataType::Boolean),
            Field::new("texc6568".into(), DataType::Boolean),
            Field::new("texc6586".into(), DataType::Boolean),
            Field::new("texc6995".into(), DataType::Boolean),
            Field::new("texc6996".into(), DataType::Boolean),
            Field::new("texc6997".into(), DataType::Boolean),
            Field::new("texc6998".into(), DataType::Boolean),
            Field::new("texc8520".into(), DataType::Float64),
            Field::new("texc8521".into(), DataType::Boolean),
            Field::new("texc8522".into(), DataType::Boolean),
            Field::new("texc8523".into(), DataType::Float64),
            Field::new("texc8524".into(), DataType::Float64),
            Field::new("texc8525".into(), DataType::Boolean),
            Field::new("texc8557".into(), DataType::Float64),
            Field::new("texc8558".into(), DataType::Float64),
            Field::new("texc8559".into(), DataType::Float64),
            Field::new("texc8562".into(), DataType::Float64),
            Field::new("texc8563".into(), DataType::Float64),
            Field::new("texc8564".into(), DataType::Float64),
            Field::new("texc8565".into(), DataType::Float64),
            Field::new("texc8566".into(), DataType::Float64),
            Field::new("texc8567".into(), DataType::Float64),
            Field::new("text3571".into(), DataType::String),
            Field::new("text3573".into(), DataType::String),
            Field::new("text3575".into(), DataType::String),
            Field::new("text4769".into(), DataType::Boolean),
            Field::new("text5351".into(), DataType::String),
            Field::new("text5352".into(), DataType::String),
            Field::new("text5353".into(), DataType::String),
            Field::new("text5354".into(), DataType::String),
            Field::new("text5355".into(), DataType::String),
            Field::new("text5356".into(), DataType::String),
            Field::new("text5357".into(), DataType::String),
            Field::new("text5358".into(), DataType::String),
            Field::new("text5359".into(), DataType::String),
            Field::new("text5360".into(), DataType::String),
            Field::new("text5485".into(), DataType::String),
            Field::new("text5486".into(), DataType::String),
            Field::new("text5487".into(), DataType::String),
            Field::new("text5488".into(), DataType::String),
            Field::new("text5489".into(), DataType::String),
            Field::new("text5523".into(), DataType::Boolean),
            Field::new("text6373".into(), DataType::String),
            Field::new("text6561".into(), DataType::String),
            Field::new("text6562".into(), DataType::String),
            Field::new("text6568".into(), DataType::String),
            Field::new("text6586".into(), DataType::String),
            Field::new("text6995".into(), DataType::Boolean),
            Field::new("text6996".into(), DataType::Boolean),
            Field::new("text6997".into(), DataType::Boolean),
            Field::new("text6998".into(), DataType::Boolean),
            Field::new("text8520".into(), DataType::String),
            Field::new("text8521".into(), DataType::String),
            Field::new("text8522".into(), DataType::String),
            Field::new("text8523".into(), DataType::String),
            Field::new("text8524".into(), DataType::String),
            Field::new("text8525".into(), DataType::String),
            Field::new("text8526".into(), DataType::String),
            Field::new("text8527".into(), DataType::String),
            Field::new("text8528".into(), DataType::String),
            Field::new("text8529".into(), DataType::String),
            Field::new("text8530".into(), DataType::String),
            Field::new("text8557".into(), DataType::String),
            Field::new("text8558".into(), DataType::String),
            Field::new("text8559".into(), DataType::String),
            Field::new("text8562".into(), DataType::String),
            Field::new("text8563".into(), DataType::String),
            Field::new("text8564".into(), DataType::String),
            Field::new("text8565".into(), DataType::String),
            Field::new("text8566".into(), DataType::String),
            Field::new("text8567".into(), DataType::String),
            Field::new("textb027".into(), DataType::String),
            Field::new("textb028".into(), DataType::String),
            Field::new("textb029".into(), DataType::String),
            Field::new("textb030".into(), DataType::String),
            Field::new("textb031".into(), DataType::String),
            Field::new("textb032".into(), DataType::String),
            Field::new("textb033".into(), DataType::String),
            Field::new("textb034".into(), DataType::String),
            Field::new("textb035".into(), DataType::String),
            Field::new("textb036".into(), DataType::String),
            Field::new("textb037".into(), DataType::String),
            Field::new("textb038".into(), DataType::String),
            Field::new("textb039".into(), DataType::String),
            Field::new("textb040".into(), DataType::String),
            Field::new("textb041".into(), DataType::String),
            Field::new("textb042".into(), DataType::String),
            Field::new("textb043".into(), DataType::String),
            Field::new("textb044".into(), DataType::String),
            Field::new("textb045".into(), DataType::String),
            Field::new("textb046".into(), DataType::String),
            Field::new("textb047".into(), DataType::String),
            Field::new("textb048".into(), DataType::String),
            Field::new("textb049".into(), DataType::String),
            Field::new("textb050".into(), DataType::String),
            Field::new("textb051".into(), DataType::String),
            Field::new("textb052".into(), DataType::String),
            Field::new("textb053".into(), DataType::String),
            Field::new("textb054".into(), DataType::Boolean),
            Field::new("textb055".into(), DataType::Boolean),
            Field::new("textb056".into(), DataType::String),
            Field::new("textc231".into(), DataType::String),
            Field::new("textc490".into(), DataType::Boolean),
            Field::new("textc497".into(), DataType::String),
            Field::new("textc703".into(), DataType::String),
            Field::new("textc708".into(), DataType::String),
            Field::new("textc714".into(), DataType::String),
            Field::new("textc715".into(), DataType::String),
            Field::new("textft29".into(), DataType::Boolean),
            Field::new("textft31".into(), DataType::String),
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
    CAST(bhbc3368 AS DOUBLE) AS bhbc3368,
    CAST(bhbc3402 AS DOUBLE) AS bhbc3402,
    CAST(bhbc3516 AS DOUBLE) AS bhbc3516,
    CAST(bhbc3519 AS DOUBLE) AS bhbc3519,
    CAST(bhbc4070 AS DOUBLE) AS bhbc4070,
    CAST(bhbc4073 AS DOUBLE) AS bhbc4073,
    CAST(bhbc4074 AS DOUBLE) AS bhbc4074,
    CAST(bhbc4079 AS DOUBLE) AS bhbc4079,
    CAST(bhbc4091 AS DOUBLE) AS bhbc4091,
    CAST(bhbc4093 AS DOUBLE) AS bhbc4093,
    CAST(bhbc4094 AS DOUBLE) AS bhbc4094,
    CAST(bhbc4107 AS DOUBLE) AS bhbc4107,
    CAST(bhbc4135 AS DOUBLE) AS bhbc4135,
    CAST(bhbc4218 AS DOUBLE) AS bhbc4218,
    CAST(bhbc4230 AS DOUBLE) AS bhbc4230,
    CAST(bhbc4301 AS DOUBLE) AS bhbc4301,
    CAST(bhbc4302 AS DOUBLE) AS bhbc4302,
    CAST(bhbc4320 AS DOUBLE) AS bhbc4320,
    CAST(bhbc4340 AS DOUBLE) AS bhbc4340,
    CAST(bhbc4421 AS DOUBLE) AS bhbc4421,
    CAST(bhbc4475 AS DOUBLE) AS bhbc4475,
    CAST(bhbc4484 AS DOUBLE) AS bhbc4484,
    CAST(bhbc4519 AS DOUBLE) AS bhbc4519,
    CAST(bhbc6061 AS DOUBLE) AS bhbc6061,
    CAST(bhbca220 AS DOUBLE) AS bhbca220,
    CAST(bhbcb490 AS DOUBLE) AS bhbcb490,
    CAST(bhbcb491 AS DOUBLE) AS bhbcb491,
    CAST(bhbcb493 AS DOUBLE) AS bhbcb493,
    CAST(bhbcb494 AS DOUBLE) AS bhbcb494,
    CAST(bhbcc216 AS DOUBLE) AS bhbcc216,
    CAST(bhbcjj33 AS DOUBLE) AS bhbcjj33,
    CAST(bhc00010 AS DOUBLE) AS bhc00010,
    CAST(bhc00390 AS DOUBLE) AS bhc00390,
    CAST(bhc01350 AS DOUBLE) AS bhc01350,
    CAST(bhc01754 AS DOUBLE) AS bhc01754,
    CAST(bhc01773 AS DOUBLE) AS bhc01773,
    CAST(bhc02122 AS DOUBLE) AS bhc02122,
    CAST(bhc02170 AS DOUBLE) AS bhc02170,
    CAST(bhc03411 AS DOUBLE) AS bhc03411,
    CAST(bhc03429 AS DOUBLE) AS bhc03429,
    CAST(bhc03433 AS DOUBLE) AS bhc03433,
    CAST(bhc03545 AS DOUBLE) AS bhc03545,
    CAST(bhc05369 AS DOUBLE) AS bhc05369,
    CAST(bhc06551 AS DOUBLE) AS bhc06551,
    CAST(bhc06563 AS DOUBLE) AS bhc06563,
    CAST(bhc06566 AS DOUBLE) AS bhc06566,
    CAST(bhc06570 AS DOUBLE) AS bhc06570,
    CAST(bhc06572 AS DOUBLE) AS bhc06572,
    CAST(bhc06574 AS DOUBLE) AS bhc06574,
    CAST(bhc06575 AS DOUBLE) AS bhc06575,
    CAST(bhc06598 AS DOUBLE) AS bhc06598,
    CAST(bhc06601 AS BOOLEAN) AS bhc06601,
    CAST(bhc06602 AS DOUBLE) AS bhc06602,
    CAST(bhc06603 AS DOUBLE) AS bhc06603,
    CAST(bhc0a167 AS DOUBLE) AS bhc0a167,
    CAST(bhc0a250 AS DOUBLE) AS bhc0a250,
    CAST(bhc0b528 AS DOUBLE) AS bhc0b528,
    CAST(bhc0b546 AS DOUBLE) AS bhc0b546,
    CAST(bhc0b639 AS DOUBLE) AS bhc0b639,
    CAST(bhc0b675 AS DOUBLE) AS bhc0b675,
    CAST(bhc0b681 AS DOUBLE) AS bhc0b681,
    CAST(bhc0c225 AS DOUBLE) AS bhc0c225,
    CAST(bhc0g591 AS DOUBLE) AS bhc0g591,
    CAST(bhc20010 AS DOUBLE) AS bhc20010,
    CAST(bhc20390 AS DOUBLE) AS bhc20390,
    CAST(bhc21350 AS DOUBLE) AS bhc21350,
    CAST(bhc21754 AS DOUBLE) AS bhc21754,
    CAST(bhc21773 AS DOUBLE) AS bhc21773,
    CAST(bhc22122 AS DOUBLE) AS bhc22122,
    CAST(bhc22170 AS DOUBLE) AS bhc22170,
    CAST(bhc23411 AS DOUBLE) AS bhc23411,
    CAST(bhc23429 AS DOUBLE) AS bhc23429,
    CAST(bhc23433 AS DOUBLE) AS bhc23433,
    CAST(bhc23545 AS DOUBLE) AS bhc23545,
    CAST(bhc25369 AS DOUBLE) AS bhc25369,
    CAST(bhc26551 AS DOUBLE) AS bhc26551,
    CAST(bhc26563 AS DOUBLE) AS bhc26563,
    CAST(bhc26566 AS DOUBLE) AS bhc26566,
    CAST(bhc26570 AS DOUBLE) AS bhc26570,
    CAST(bhc26572 AS DOUBLE) AS bhc26572,
    CAST(bhc26574 AS DOUBLE) AS bhc26574,
    CAST(bhc26575 AS DOUBLE) AS bhc26575,
    CAST(bhc26598 AS DOUBLE) AS bhc26598,
    CAST(bhc26601 AS DOUBLE) AS bhc26601,
    CAST(bhc26602 AS DOUBLE) AS bhc26602,
    CAST(bhc26603 AS DOUBLE) AS bhc26603,
    CAST(bhc2a167 AS DOUBLE) AS bhc2a167,
    CAST(bhc2a250 AS DOUBLE) AS bhc2a250,
    CAST(bhc2b528 AS DOUBLE) AS bhc2b528,
    CAST(bhc2b546 AS DOUBLE) AS bhc2b546,
    CAST(bhc2b639 AS DOUBLE) AS bhc2b639,
    CAST(bhc2b675 AS DOUBLE) AS bhc2b675,
    CAST(bhc2b681 AS DOUBLE) AS bhc2b681,
    CAST(bhc2c225 AS DOUBLE) AS bhc2c225,
    CAST(bhc2g591 AS DOUBLE) AS bhc2g591,
    CAST(bhc50390 AS DOUBLE) AS bhc50390,
    CAST(bhc51350 AS BOOLEAN) AS bhc51350,
    CAST(bhc51754 AS DOUBLE) AS bhc51754,
    CAST(bhc51773 AS DOUBLE) AS bhc51773,
    CAST(bhc52122 AS DOUBLE) AS bhc52122,
    CAST(bhc52170 AS DOUBLE) AS bhc52170,
    CAST(bhc53411 AS DOUBLE) AS bhc53411,
    CAST(bhc53433 AS DOUBLE) AS bhc53433,
    CAST(bhc53545 AS DOUBLE) AS bhc53545,
    CAST(bhc55369 AS DOUBLE) AS bhc55369,
    CAST(bhc56551 AS DOUBLE) AS bhc56551,
    CAST(bhc56563 AS DOUBLE) AS bhc56563,
    CAST(bhc56566 AS DOUBLE) AS bhc56566,
    CAST(bhc56570 AS DOUBLE) AS bhc56570,
    CAST(bhc56572 AS DOUBLE) AS bhc56572,
    CAST(bhc56574 AS DOUBLE) AS bhc56574,
    CAST(bhc56575 AS DOUBLE) AS bhc56575,
    CAST(bhc56598 AS DOUBLE) AS bhc56598,
    CAST(bhc56602 AS DOUBLE) AS bhc56602,
    CAST(bhc56603 AS DOUBLE) AS bhc56603,
    CAST(bhc5a167 AS DOUBLE) AS bhc5a167,
    CAST(bhc5a250 AS DOUBLE) AS bhc5a250,
    CAST(bhc5b528 AS DOUBLE) AS bhc5b528,
    CAST(bhc5b546 AS DOUBLE) AS bhc5b546,
    CAST(bhc5b639 AS DOUBLE) AS bhc5b639,
    CAST(bhc5b675 AS DOUBLE) AS bhc5b675,
    CAST(bhc5b681 AS DOUBLE) AS bhc5b681,
    CAST(bhc5g591 AS DOUBLE) AS bhc5g591,
    CAST(bhc90010 AS DOUBLE) AS bhc90010,
    CAST(bhc90390 AS DOUBLE) AS bhc90390,
    CAST(bhc91350 AS DOUBLE) AS bhc91350,
    CAST(bhc91727 AS DOUBLE) AS bhc91727,
    CAST(bhc91754 AS DOUBLE) AS bhc91754,
    CAST(bhc91773 AS DOUBLE) AS bhc91773,
    CAST(bhc92122 AS DOUBLE) AS bhc92122,
    CAST(bhc92170 AS DOUBLE) AS bhc92170,
    CAST(bhc93411 AS DOUBLE) AS bhc93411,
    CAST(bhc93429 AS DOUBLE) AS bhc93429,
    CAST(bhc93433 AS DOUBLE) AS bhc93433,
    CAST(bhc93545 AS DOUBLE) AS bhc93545,
    CAST(bhc95369 AS DOUBLE) AS bhc95369,
    CAST(bhc96551 AS DOUBLE) AS bhc96551,
    CAST(bhc96563 AS DOUBLE) AS bhc96563,
    CAST(bhc96566 AS DOUBLE) AS bhc96566,
    CAST(bhc96570 AS DOUBLE) AS bhc96570,
    CAST(bhc96572 AS DOUBLE) AS bhc96572,
    CAST(bhc96574 AS DOUBLE) AS bhc96574,
    CAST(bhc96575 AS DOUBLE) AS bhc96575,
    CAST(bhc96598 AS DOUBLE) AS bhc96598,
    CAST(bhc96602 AS DOUBLE) AS bhc96602,
    CAST(bhc96603 AS DOUBLE) AS bhc96603,
    CAST(bhc9a250 AS DOUBLE) AS bhc9a250,
    CAST(bhc9b528 AS DOUBLE) AS bhc9b528,
    CAST(bhc9b541 AS DOUBLE) AS bhc9b541,
    CAST(bhc9b546 AS DOUBLE) AS bhc9b546,
    CAST(bhc9b639 AS DOUBLE) AS bhc9b639,
    CAST(bhc9b675 AS DOUBLE) AS bhc9b675,
    CAST(bhc9b681 AS DOUBLE) AS bhc9b681,
    CAST(bhc9c225 AS DOUBLE) AS bhc9c225,
    CAST(bhc9g591 AS DOUBLE) AS bhc9g591,
    CAST(bhca2170 AS DOUBLE) AS bhca2170,
    CAST(bhca3792 AS DOUBLE) AS bhca3792,
    CAST(bhca5310 AS DOUBLE) AS bhca5310,
    CAST(bhca5311 AS DOUBLE) AS bhca5311,
    CAST(bhca7204 AS DOUBLE) AS bhca7204,
    CAST(bhca7205 AS DOUBLE) AS bhca7205,
    CAST(bhca7206 AS DOUBLE) AS bhca7206,
    CAST(bhca8274 AS DOUBLE) AS bhca8274,
    CAST(bhcaa223 AS DOUBLE) AS bhcaa223,
    CAST(bhcaa224 AS DOUBLE) AS bhcaa224,
    CAST(bhcab530 AS DOUBLE) AS bhcab530,
    CAST(bhcab596 AS DOUBLE) AS bhcab596,
    CAST(bhcah036 AS DOUBLE) AS bhcah036,
    CAST(bhcah311 AS DOUBLE) AS bhcah311,
    CAST(bhcah312 AS BOOLEAN) AS bhcah312,
    CAST(bhcah313 AS DOUBLE) AS bhcah313,
    CAST(bhcah314 AS DOUBLE) AS bhcah314,
    CAST(bhcajj29 AS DOUBLE) AS bhcajj29,
    CAST(bhcakw00 AS DOUBLE) AS bhcakw00,
    CAST(bhcakw03 AS DOUBLE) AS bhcakw03,
    CAST(bhcakx77 AS DOUBLE) AS bhcakx77,
    CAST(bhcakx78 AS DOUBLE) AS bhcakx78,
    CAST(bhcakx79 AS DOUBLE) AS bhcakx79,
    CAST(bhcakx80 AS DOUBLE) AS bhcakx80,
    CAST(bhcakx81 AS DOUBLE) AS bhcakx81,
    CAST(bhcakx82 AS DOUBLE) AS bhcakx82,
    CAST(bhcakx83 AS DOUBLE) AS bhcakx83,
    CAST(bhcalb58 AS DOUBLE) AS bhcalb58,
    CAST(bhcalb59 AS DOUBLE) AS bhcalb59,
    CAST(bhcalb60 AS DOUBLE) AS bhcalb60,
    CAST(bhcalb61 AS DOUBLE) AS bhcalb61,
    CAST(bhcale74 AS DOUBLE) AS bhcale74,
    CAST(bhcale85 AS DOUBLE) AS bhcale85,
    CAST(bhcale86 AS DOUBLE) AS bhcale86,
    CAST(bhcale87 AS DOUBLE) AS bhcale87,
    CAST(bhcale88 AS DOUBLE) AS bhcale88,
    CAST(bhcale89 AS DOUBLE) AS bhcale89,
    CAST(bhcale90 AS DOUBLE) AS bhcale90,
    CAST(bhcale91 AS DOUBLE) AS bhcale91,
    CAST(bhcale92 AS DOUBLE) AS bhcale92,
    CAST(bhcalf21 AS DOUBLE) AS bhcalf21,
    CAST(bhcalf22 AS DOUBLE) AS bhcalf22,
    CAST(bhcalf23 AS DOUBLE) AS bhcalf23,
    CAST(bhcalf24 AS DOUBLE) AS bhcalf24,
    CAST(bhcalf25 AS DOUBLE) AS bhcalf25,
    CAST(bhcalf27 AS DOUBLE) AS bhcalf27,
    CAST(bhcalf28 AS DOUBLE) AS bhcalf28,
    CAST(bhcamk66 AS DOUBLE) AS bhcamk66,
    CAST(bhcamk76 AS DOUBLE) AS bhcamk76,
    CAST(bhcamk77 AS DOUBLE) AS bhcamk77,
    CAST(bhcamk78 AS DOUBLE) AS bhcamk78,
    CAST(bhcanc99 AS DOUBLE) AS bhcanc99,
    CAST(bhcap742 AS DOUBLE) AS bhcap742,
    CAST(bhcap793 AS DOUBLE) AS bhcap793,
    CAST(bhcap838 AS DOUBLE) AS bhcap838,
    CAST(bhcap839 AS DOUBLE) AS bhcap839,
    CAST(bhcap840 AS DOUBLE) AS bhcap840,
    CAST(bhcap841 AS DOUBLE) AS bhcap841,
    CAST(bhcap842 AS DOUBLE) AS bhcap842,
    CAST(bhcap843 AS DOUBLE) AS bhcap843,
    CAST(bhcap844 AS DOUBLE) AS bhcap844,
    CAST(bhcap845 AS DOUBLE) AS bhcap845,
    CAST(bhcap846 AS DOUBLE) AS bhcap846,
    CAST(bhcap847 AS DOUBLE) AS bhcap847,
    CAST(bhcap848 AS DOUBLE) AS bhcap848,
    CAST(bhcap849 AS DOUBLE) AS bhcap849,
    CAST(bhcap850 AS DOUBLE) AS bhcap850,
    CAST(bhcap851 AS DOUBLE) AS bhcap851,
    CAST(bhcap852 AS DOUBLE) AS bhcap852,
    CAST(bhcap853 AS DOUBLE) AS bhcap853,
    CAST(bhcap854 AS DOUBLE) AS bhcap854,
    CAST(bhcap855 AS DOUBLE) AS bhcap855,
    CAST(bhcap856 AS DOUBLE) AS bhcap856,
    CAST(bhcap857 AS DOUBLE) AS bhcap857,
    CAST(bhcap858 AS DOUBLE) AS bhcap858,
    CAST(bhcap859 AS DOUBLE) AS bhcap859,
    CAST(bhcap860 AS DOUBLE) AS bhcap860,
    CAST(bhcap861 AS DOUBLE) AS bhcap861,
    CAST(bhcap862 AS DOUBLE) AS bhcap862,
    CAST(bhcap863 AS DOUBLE) AS bhcap863,
    CAST(bhcap864 AS DOUBLE) AS bhcap864,
    CAST(bhcap865 AS DOUBLE) AS bhcap865,
    CAST(bhcap866 AS DOUBLE) AS bhcap866,
    CAST(bhcap867 AS DOUBLE) AS bhcap867,
    CAST(bhcap868 AS DOUBLE) AS bhcap868,
    CAST(bhcap870 AS DOUBLE) AS bhcap870,
    CAST(bhcap872 AS DOUBLE) AS bhcap872,
    CAST(bhcap875 AS DOUBLE) AS bhcap875,
    CAST(bhcaq257 AS DOUBLE) AS bhcaq257,
    CAST(bhcaq258 AS DOUBLE) AS bhcaq258,
    CAST(bhcas540 AS DOUBLE) AS bhcas540,
    CAST(bhcb2210 AS DOUBLE) AS bhcb2210,
    CAST(bhcb2389 AS DOUBLE) AS bhcb2389,
    CAST(bhcb2604 AS DOUBLE) AS bhcb2604,
    CAST(bhcb3187 AS DOUBLE) AS bhcb3187,
    CAST(bhcb6648 AS DOUBLE) AS bhcb6648,
    CAST(bhcbhk29 AS DOUBLE) AS bhcbhk29,
    CAST(bhcbj474 AS DOUBLE) AS bhcbj474,
    CAST(bhce0010 AS DOUBLE) AS bhce0010,
    CAST(bhce1727 AS DOUBLE) AS bhce1727,
    CAST(bhce1754 AS DOUBLE) AS bhce1754,
    CAST(bhce1773 AS DOUBLE) AS bhce1773,
    CAST(bhce2170 AS DOUBLE) AS bhce2170,
    CAST(bhce3123 AS DOUBLE) AS bhce3123,
    CAST(bhce3411 AS DOUBLE) AS bhce3411,
    CAST(bhce3429 AS DOUBLE) AS bhce3429,
    CAST(bhce3433 AS DOUBLE) AS bhce3433,
    CAST(bhce3545 AS DOUBLE) AS bhce3545,
    CAST(bhce5369 AS DOUBLE) AS bhce5369,
    CAST(bhce6566 AS DOUBLE) AS bhce6566,
    CAST(bhce6570 AS DOUBLE) AS bhce6570,
    CAST(bhce6572 AS DOUBLE) AS bhce6572,
    CAST(bhcea167 AS DOUBLE) AS bhcea167,
    CAST(bhcea250 AS DOUBLE) AS bhcea250,
    CAST(bhceb528 AS DOUBLE) AS bhceb528,
    CAST(bhceb541 AS DOUBLE) AS bhceb541,
    CAST(bhceb546 AS DOUBLE) AS bhceb546,
    CAST(bhceb639 AS DOUBLE) AS bhceb639,
    CAST(bhceb675 AS DOUBLE) AS bhceb675,
    CAST(bhceb681 AS DOUBLE) AS bhceb681,
    CAST(bhceg591 AS DOUBLE) AS bhceg591,
    CAST(bhcm3531 AS DOUBLE) AS bhcm3531,
    CAST(bhcm3532 AS DOUBLE) AS bhcm3532,
    CAST(bhcm3533 AS DOUBLE) AS bhcm3533,
    CAST(bhcm3534 AS DOUBLE) AS bhcm3534,
    CAST(bhcm3535 AS DOUBLE) AS bhcm3535,
    CAST(bhcm3536 AS DOUBLE) AS bhcm3536,
    CAST(bhcm3537 AS DOUBLE) AS bhcm3537,
    CAST(bhcm3541 AS DOUBLE) AS bhcm3541,
    CAST(bhcm3543 AS DOUBLE) AS bhcm3543,
    CAST(bhcp0010 AS DOUBLE) AS bhcp0010,
    CAST(bhcp0087 AS DOUBLE) AS bhcp0087,
    CAST(bhcp0201 AS DOUBLE) AS bhcp0201,
    CAST(bhcp0202 AS DOUBLE) AS bhcp0202,
    CAST(bhcp0203 AS DOUBLE) AS bhcp0203,
    CAST(bhcp0204 AS DOUBLE) AS bhcp0204,
    CAST(bhcp0205 AS DOUBLE) AS bhcp0205,
    CAST(bhcp0206 AS DOUBLE) AS bhcp0206,
    CAST(bhcp0207 AS DOUBLE) AS bhcp0207,
    CAST(bhcp0208 AS DOUBLE) AS bhcp0208,
    CAST(bhcp0209 AS DOUBLE) AS bhcp0209,
    CAST(bhcp0210 AS DOUBLE) AS bhcp0210,
    CAST(bhcp0277 AS DOUBLE) AS bhcp0277,
    CAST(bhcp0279 AS DOUBLE) AS bhcp0279,
    CAST(bhcp0362 AS DOUBLE) AS bhcp0362,
    CAST(bhcp0363 AS DOUBLE) AS bhcp0363,
    CAST(bhcp0364 AS DOUBLE) AS bhcp0364,
    CAST(bhcp0365 AS DOUBLE) AS bhcp0365,
    CAST(bhcp0368 AS DOUBLE) AS bhcp0368,
    CAST(bhcp0400 AS DOUBLE) AS bhcp0400,
    CAST(bhcp0416 AS DOUBLE) AS bhcp0416,
    CAST(bhcp0447 AS DOUBLE) AS bhcp0447,
    CAST(bhcp0467 AS DOUBLE) AS bhcp0467,
    CAST(bhcp0496 AS DOUBLE) AS bhcp0496,
    CAST(bhcp0508 AS DOUBLE) AS bhcp0508,
    CAST(bhcp0512 AS DOUBLE) AS bhcp0512,
    CAST(bhcp0515 AS DOUBLE) AS bhcp0515,
    CAST(bhcp0518 AS DOUBLE) AS bhcp0518,
    CAST(bhcp0520 AS DOUBLE) AS bhcp0520,
    CAST(bhcp0522 AS DOUBLE) AS bhcp0522,
    CAST(bhcp0533 AS DOUBLE) AS bhcp0533,
    CAST(bhcp0534 AS DOUBLE) AS bhcp0534,
    CAST(bhcp0536 AS DOUBLE) AS bhcp0536,
    CAST(bhcp0537 AS DOUBLE) AS bhcp0537,
    CAST(bhcp0538 AS DOUBLE) AS bhcp0538,
    CAST(bhcp0539 AS DOUBLE) AS bhcp0539,
    CAST(bhcp0540 AS DOUBLE) AS bhcp0540,
    CAST(bhcp0541 AS DOUBLE) AS bhcp0541,
    CAST(bhcp0542 AS DOUBLE) AS bhcp0542,
    CAST(bhcp0543 AS DOUBLE) AS bhcp0543,
    CAST(bhcp1273 AS DOUBLE) AS bhcp1273,
    CAST(bhcp1274 AS DOUBLE) AS bhcp1274,
    CAST(bhcp1275 AS DOUBLE) AS bhcp1275,
    CAST(bhcp1276 AS DOUBLE) AS bhcp1276,
    CAST(bhcp1277 AS DOUBLE) AS bhcp1277,
    CAST(bhcp1278 AS DOUBLE) AS bhcp1278,
    CAST(bhcp1279 AS DOUBLE) AS bhcp1279,
    CAST(bhcp1299 AS DOUBLE) AS bhcp1299,
    CAST(bhcp1403 AS DOUBLE) AS bhcp1403,
    CAST(bhcp1407 AS DOUBLE) AS bhcp1407,
    CAST(bhcp1616 AS DOUBLE) AS bhcp1616,
    CAST(bhcp2123 AS DOUBLE) AS bhcp2123,
    CAST(bhcp2125 AS DOUBLE) AS bhcp2125,
    CAST(bhcp2145 AS DOUBLE) AS bhcp2145,
    CAST(bhcp2160 AS DOUBLE) AS bhcp2160,
    CAST(bhcp2165 AS DOUBLE) AS bhcp2165,
    CAST(bhcp2170 AS DOUBLE) AS bhcp2170,
    CAST(bhcp2200 AS DOUBLE) AS bhcp2200,
    CAST(bhcp2309 AS DOUBLE) AS bhcp2309,
    CAST(bhcp2332 AS DOUBLE) AS bhcp2332,
    CAST(bhcp2792 AS DOUBLE) AS bhcp2792,
    CAST(bhcp2793 AS DOUBLE) AS bhcp2793,
    CAST(bhcp2794 AS DOUBLE) AS bhcp2794,
    CAST(bhcp2796 AS DOUBLE) AS bhcp2796,
    CAST(bhcp2831 AS DOUBLE) AS bhcp2831,
    CAST(bhcp2930 AS DOUBLE) AS bhcp2930,
    CAST(bhcp3123 AS DOUBLE) AS bhcp3123,
    CAST(bhcp3128 AS DOUBLE) AS bhcp3128,
    CAST(bhcp3147 AS DOUBLE) AS bhcp3147,
    CAST(bhcp3152 AS DOUBLE) AS bhcp3152,
    CAST(bhcp3153 AS DOUBLE) AS bhcp3153,
    CAST(bhcp3156 AS DOUBLE) AS bhcp3156,
    CAST(bhcp3163 AS DOUBLE) AS bhcp3163,
    CAST(bhcp3164 AS DOUBLE) AS bhcp3164,
    CAST(bhcp3165 AS DOUBLE) AS bhcp3165,
    CAST(bhcp3210 AS DOUBLE) AS bhcp3210,
    CAST(bhcp3230 AS DOUBLE) AS bhcp3230,
    CAST(bhcp3238 AS DOUBLE) AS bhcp3238,
    CAST(bhcp3239 AS DOUBLE) AS bhcp3239,
    CAST(bhcp3240 AS DOUBLE) AS bhcp3240,
    CAST(bhcp3247 AS DOUBLE) AS bhcp3247,
    CAST(bhcp3283 AS DOUBLE) AS bhcp3283,
    CAST(bhcp3290 AS DOUBLE) AS bhcp3290,
    CAST(bhcp3293 AS DOUBLE) AS bhcp3293,
    CAST(bhcp3298 AS DOUBLE) AS bhcp3298,
    CAST(bhcp3300 AS DOUBLE) AS bhcp3300,
    CAST(bhcp3409 AS DOUBLE) AS bhcp3409,
    CAST(bhcp3513 AS DOUBLE) AS bhcp3513,
    CAST(bhcp3602 AS DOUBLE) AS bhcp3602,
    CAST(bhcp3603 AS DOUBLE) AS bhcp3603,
    CAST(bhcp3604 AS DOUBLE) AS bhcp3604,
    CAST(bhcp3605 AS DOUBLE) AS bhcp3605,
    CAST(bhcp3606 AS DOUBLE) AS bhcp3606,
    CAST(bhcp3607 AS DOUBLE) AS bhcp3607,
    CAST(bhcp3609 AS DOUBLE) AS bhcp3609,
    CAST(bhcp3611 AS DOUBLE) AS bhcp3611,
    CAST(bhcp3612 AS DOUBLE) AS bhcp3612,
    CAST(bhcp3613 AS DOUBLE) AS bhcp3613,
    CAST(bhcp3614 AS DOUBLE) AS bhcp3614,
    CAST(bhcp3615 AS DOUBLE) AS bhcp3615,
    CAST(bhcp3616 AS DOUBLE) AS bhcp3616,
    CAST(bhcp3617 AS DOUBLE) AS bhcp3617,
    CAST(bhcp3618 AS DOUBLE) AS bhcp3618,
    CAST(bhcp3619 AS DOUBLE) AS bhcp3619,
    CAST(bhcp4000 AS DOUBLE) AS bhcp4000,
    CAST(bhcp4062 AS DOUBLE) AS bhcp4062,
    CAST(bhcp4073 AS DOUBLE) AS bhcp4073,
    CAST(bhcp4091 AS DOUBLE) AS bhcp4091,
    CAST(bhcp4130 AS DOUBLE) AS bhcp4130,
    CAST(bhcp4135 AS DOUBLE) AS bhcp4135,
    CAST(bhcp4230 AS DOUBLE) AS bhcp4230,
    CAST(bhcp4243 AS DOUBLE) AS bhcp4243,
    CAST(bhcp4250 AS DOUBLE) AS bhcp4250,
    CAST(bhcp4302 AS DOUBLE) AS bhcp4302,
    CAST(bhcp4320 AS DOUBLE) AS bhcp4320,
    CAST(bhcp4336 AS DOUBLE) AS bhcp4336,
    CAST(bhcp4340 AS DOUBLE) AS bhcp4340,
    CAST(bhcp4485 AS DOUBLE) AS bhcp4485,
    CAST(bhcp4605 AS DOUBLE) AS bhcp4605,
    CAST(bhcp4635 AS DOUBLE) AS bhcp4635,
    CAST(bhcp4647 AS DOUBLE) AS bhcp4647,
    CAST(bhcp4778 AS DOUBLE) AS bhcp4778,
    CAST(bhcp5485 AS DOUBLE) AS bhcp5485,
    CAST(bhcp5486 AS DOUBLE) AS bhcp5486,
    CAST(bhcp5487 AS DOUBLE) AS bhcp5487,
    CAST(bhcp5488 AS DOUBLE) AS bhcp5488,
    CAST(bhcp5489 AS DOUBLE) AS bhcp5489,
    CAST(bhcp5993 AS DOUBLE) AS bhcp5993,
    CAST(bhcp6552 AS DOUBLE) AS bhcp6552,
    CAST(bhcp6567 AS DOUBLE) AS bhcp6567,
    CAST(bhcp6571 AS DOUBLE) AS bhcp6571,
    CAST(bhcp6573 AS DOUBLE) AS bhcp6573,
    CAST(bhcp6588 AS DOUBLE) AS bhcp6588,
    CAST(bhcp6589 AS DOUBLE) AS bhcp6589,
    CAST(bhcp6590 AS DOUBLE) AS bhcp6590,
    CAST(bhcp6591 AS DOUBLE) AS bhcp6591,
    CAST(bhcp6592 AS DOUBLE) AS bhcp6592,
    CAST(bhcp6596 AS DOUBLE) AS bhcp6596,
    CAST(bhcp6600 AS DOUBLE) AS bhcp6600,
    CAST(bhcp6604 AS DOUBLE) AS bhcp6604,
    CAST(bhcp6607 AS DOUBLE) AS bhcp6607,
    CAST(bhcp6619 AS DOUBLE) AS bhcp6619,
    CAST(bhcp6649 AS DOUBLE) AS bhcp6649,
    CAST(bhcp6741 AS DOUBLE) AS bhcp6741,
    CAST(bhcp6742 AS DOUBLE) AS bhcp6742,
    CAST(bhcp6743 AS DOUBLE) AS bhcp6743,
    CAST(bhcp6744 AS DOUBLE) AS bhcp6744,
    CAST(bhcp6758 AS DOUBLE) AS bhcp6758,
    CAST(bhcp6773 AS DOUBLE) AS bhcp6773,
    CAST(bhcp6775 AS DOUBLE) AS bhcp6775,
    CAST(bhcp6791 AS DOUBLE) AS bhcp6791,
    CAST(bhcp6792 AS DOUBLE) AS bhcp6792,
    CAST(bhcp6793 AS DOUBLE) AS bhcp6793,
    CAST(bhcp6794 AS DOUBLE) AS bhcp6794,
    CAST(bhcp6795 AS DOUBLE) AS bhcp6795,
    CAST(bhcp8434 AS DOUBLE) AS bhcp8434,
    CAST(bhcp8516 AS DOUBLE) AS bhcp8516,
    CAST(bhcp8517 AS DOUBLE) AS bhcp8517,
    CAST(bhcp8518 AS DOUBLE) AS bhcp8518,
    CAST(bhcp8843 AS DOUBLE) AS bhcp8843,
    CAST(bhcp9191 AS DOUBLE) AS bhcp9191,
    CAST(bhcp9802 AS DOUBLE) AS bhcp9802,
    CAST(bhcpa130 AS DOUBLE) AS bhcpa130,
    CAST(bhcpb530 AS DOUBLE) AS bhcpb530,
    CAST(bhcpc254 AS DOUBLE) AS bhcpc254,
    CAST(bhcpc255 AS DOUBLE) AS bhcpc255,
    CAST(bhcpc427 AS DOUBLE) AS bhcpc427,
    CAST(bhcpc428 AS DOUBLE) AS bhcpc428,
    CAST(bhcpc447 AS DOUBLE) AS bhcpc447,
    CAST(bhcpf229 AS DOUBLE) AS bhcpf229,
    CAST(bhcpf737 AS DOUBLE) AS bhcpf737,
    CAST(bhcpf817 AS DOUBLE) AS bhcpf817,
    CAST(bhcpf818 AS DOUBLE) AS bhcpf818,
    CAST(bhcpf819 AS DOUBLE) AS bhcpf819,
    CAST(bhcpf820 AS DOUBLE) AS bhcpf820,
    CAST(bhcpf838 AS DOUBLE) AS bhcpf838,
    CAST(bhcpf841 AS BOOLEAN) AS bhcpf841,
    CAST(bhcpf842 AS BOOLEAN) AS bhcpf842,
    CAST(bhcpft28 AS DOUBLE) AS bhcpft28,
    CAST(bhcphk02 AS DOUBLE) AS bhcphk02,
    CAST(bhcpht69 AS DOUBLE) AS bhcpht69,
    CAST(bhcpht70 AS DOUBLE) AS bhcpht70,
    CAST(bhcphu25 AS DOUBLE) AS bhcphu25,
    CAST(bhcphu26 AS DOUBLE) AS bhcphu26,
    CAST(bhcpj980 AS DOUBLE) AS bhcpj980,
    CAST(bhcpja22 AS DOUBLE) AS bhcpja22,
    CAST(bhcpjj33 AS DOUBLE) AS bhcpjj33,
    CAST(bhcpk297 AS DOUBLE) AS bhcpk297,
    CAST(bhcpky38 AS BOOLEAN) AS bhcpky38,
    CAST(bhcpm962 AS DOUBLE) AS bhcpm962,
    CAST(bhct0426 AS DOUBLE) AS bhct0426,
    CAST(bhct1754 AS DOUBLE) AS bhct1754,
    CAST(bhct1773 AS DOUBLE) AS bhct1773,
    CAST(bhct2143 AS DOUBLE) AS bhct2143,
    CAST(bhct2150 AS DOUBLE) AS bhct2150,
    CAST(bhct2160 AS DOUBLE) AS bhct2160,
    CAST(bhct2170 AS DOUBLE) AS bhct2170,
    CAST(bhct2750 AS DOUBLE) AS bhct2750,
    CAST(bhct3123 AS DOUBLE) AS bhct3123,
    CAST(bhct3190 AS DOUBLE) AS bhct3190,
    CAST(bhct3210 AS DOUBLE) AS bhct3210,
    CAST(bhct3247 AS DOUBLE) AS bhct3247,
    CAST(bhct3368 AS DOUBLE) AS bhct3368,
    CAST(bhct3411 AS DOUBLE) AS bhct3411,
    CAST(bhct3433 AS DOUBLE) AS bhct3433,
    CAST(bhct3543 AS DOUBLE) AS bhct3543,
    CAST(bhct3545 AS DOUBLE) AS bhct3545,
    CAST(bhct3547 AS DOUBLE) AS bhct3547,
    CAST(bhct3548 AS DOUBLE) AS bhct3548,
    CAST(bhct4230 AS DOUBLE) AS bhct4230,
    CAST(bhct4340 AS DOUBLE) AS bhct4340,
    CAST(bhct4605 AS DOUBLE) AS bhct4605,
    CAST(bhct5369 AS DOUBLE) AS bhct5369,
    CAST(bhct5610 AS DOUBLE) AS bhct5610,
    CAST(bhct6570 AS DOUBLE) AS bhct6570,
    CAST(bhcta250 AS DOUBLE) AS bhcta250,
    CAST(bhctb528 AS DOUBLE) AS bhctb528,
    CAST(bhctb590 AS DOUBLE) AS bhctb590,
    CAST(bhctb591 AS DOUBLE) AS bhctb591,
    CAST(bhcw3792 AS DOUBLE) AS bhcw3792,
    CAST(bhcw5310 AS DOUBLE) AS bhcw5310,
    CAST(bhcw5311 AS DOUBLE) AS bhcw5311,
    CAST(bhcw7205 AS DOUBLE) AS bhcw7205,
    CAST(bhcw7206 AS DOUBLE) AS bhcw7206,
    CAST(bhcwa223 AS DOUBLE) AS bhcwa223,
    CAST(bhcwh311 AS DOUBLE) AS bhcwh311,
    CAST(bhcwkx78 AS BOOLEAN) AS bhcwkx78,
    CAST(bhcwkx83 AS BOOLEAN) AS bhcwkx83,
    CAST(bhcwle85 AS DOUBLE) AS bhcwle85,
    CAST(bhcwle86 AS DOUBLE) AS bhcwle86,
    CAST(bhcwle87 AS DOUBLE) AS bhcwle87,
    CAST(bhcwlf23 AS DOUBLE) AS bhcwlf23,
    CAST(bhcwlf24 AS DOUBLE) AS bhcwlf24,
    CAST(bhcwlf25 AS DOUBLE) AS bhcwlf25,
    CAST(bhcwmk66 AS DOUBLE) AS bhcwmk66,
    CAST(bhcwp793 AS DOUBLE) AS bhcwp793,
    CAST(bhcwp851 AS DOUBLE) AS bhcwp851,
    CAST(bhcwp852 AS DOUBLE) AS bhcwp852,
    CAST(bhcwp853 AS DOUBLE) AS bhcwp853,
    CAST(bhcwp854 AS DOUBLE) AS bhcwp854,
    CAST(bhcwp855 AS DOUBLE) AS bhcwp855,
    CAST(bhcwp856 AS DOUBLE) AS bhcwp856,
    CAST(bhcwp857 AS DOUBLE) AS bhcwp857,
    CAST(bhcwp858 AS DOUBLE) AS bhcwp858,
    CAST(bhcwp859 AS DOUBLE) AS bhcwp859,
    CAST(bhcwp870 AS DOUBLE) AS bhcwp870,
    CAST(bhcx1754 AS DOUBLE) AS bhcx1754,
    CAST(bhcx1773 AS DOUBLE) AS bhcx1773,
    CAST(bhcx3123 AS DOUBLE) AS bhcx3123,
    CAST(bhcx3210 AS DOUBLE) AS bhcx3210,
    CAST(bhcx3368 AS DOUBLE) AS bhcx3368,
    CAST(bhcx3545 AS DOUBLE) AS bhcx3545,
    CAST(bhcy1773 AS DOUBLE) AS bhcy1773,
    CAST(bhcy3123 AS DOUBLE) AS bhcy3123,
    CAST(bhcyja36 AS DOUBLE) AS bhcyja36,
    CAST(bhdm1288 AS DOUBLE) AS bhdm1288,
    CAST(bhdm1410 AS DOUBLE) AS bhdm1410,
    CAST(bhdm1415 AS DOUBLE) AS bhdm1415,
    CAST(bhdm1420 AS DOUBLE) AS bhdm1420,
    CAST(bhdm1460 AS DOUBLE) AS bhdm1460,
    CAST(bhdm1480 AS DOUBLE) AS bhdm1480,
    CAST(bhdm1545 AS DOUBLE) AS bhdm1545,
    CAST(bhdm1564 AS DOUBLE) AS bhdm1564,
    CAST(bhdm1590 AS DOUBLE) AS bhdm1590,
    CAST(bhdm1635 AS DOUBLE) AS bhdm1635,
    CAST(bhdm1755 AS DOUBLE) AS bhdm1755,
    CAST(bhdm1766 AS DOUBLE) AS bhdm1766,
    CAST(bhdm1797 AS DOUBLE) AS bhdm1797,
    CAST(bhdm1975 AS DOUBLE) AS bhdm1975,
    CAST(bhdm2081 AS DOUBLE) AS bhdm2081,
    CAST(bhdm2122 AS DOUBLE) AS bhdm2122,
    CAST(bhdm2123 AS DOUBLE) AS bhdm2123,
    CAST(bhdm2165 AS DOUBLE) AS bhdm2165,
    CAST(bhdm3386 AS DOUBLE) AS bhdm3386,
    CAST(bhdm3387 AS DOUBLE) AS bhdm3387,
    CAST(bhdm3465 AS DOUBLE) AS bhdm3465,
    CAST(bhdm3466 AS DOUBLE) AS bhdm3466,
    CAST(bhdm3516 AS DOUBLE) AS bhdm3516,
    CAST(bhdm3545 AS DOUBLE) AS bhdm3545,
    CAST(bhdm3546 AS DOUBLE) AS bhdm3546,
    CAST(bhdm3547 AS DOUBLE) AS bhdm3547,
    CAST(bhdm3548 AS DOUBLE) AS bhdm3548,
    CAST(bhdm5367 AS DOUBLE) AS bhdm5367,
    CAST(bhdm5368 AS DOUBLE) AS bhdm5368,
    CAST(bhdm6631 AS DOUBLE) AS bhdm6631,
    CAST(bhdm6636 AS DOUBLE) AS bhdm6636,
    CAST(bhdma164 AS DOUBLE) AS bhdma164,
    CAST(bhdma242 AS DOUBLE) AS bhdma242,
    CAST(bhdma243 AS DOUBLE) AS bhdma243,
    CAST(bhdmb561 AS DOUBLE) AS bhdmb561,
    CAST(bhdmb562 AS DOUBLE) AS bhdmb562,
    CAST(bhdmb987 AS DOUBLE) AS bhdmb987,
    CAST(bhdmb993 AS DOUBLE) AS bhdmb993,
    CAST(bhdmf560 AS DOUBLE) AS bhdmf560,
    CAST(bhdmf576 AS DOUBLE) AS bhdmf576,
    CAST(bhdmf577 AS DOUBLE) AS bhdmf577,
    CAST(bhdmf578 AS DOUBLE) AS bhdmf578,
    CAST(bhdmf579 AS DOUBLE) AS bhdmf579,
    CAST(bhdmf580 AS DOUBLE) AS bhdmf580,
    CAST(bhdmf581 AS DOUBLE) AS bhdmf581,
    CAST(bhdmf582 AS DOUBLE) AS bhdmf582,
    CAST(bhdmf583 AS DOUBLE) AS bhdmf583,
    CAST(bhdmf584 AS DOUBLE) AS bhdmf584,
    CAST(bhdmf585 AS DOUBLE) AS bhdmf585,
    CAST(bhdmf586 AS DOUBLE) AS bhdmf586,
    CAST(bhdmf587 AS DOUBLE) AS bhdmf587,
    CAST(bhdmf588 AS DOUBLE) AS bhdmf588,
    CAST(bhdmf589 AS DOUBLE) AS bhdmf589,
    CAST(bhdmf590 AS DOUBLE) AS bhdmf590,
    CAST(bhdmf591 AS DOUBLE) AS bhdmf591,
    CAST(bhdmf592 AS DOUBLE) AS bhdmf592,
    CAST(bhdmf593 AS DOUBLE) AS bhdmf593,
    CAST(bhdmf594 AS DOUBLE) AS bhdmf594,
    CAST(bhdmf595 AS DOUBLE) AS bhdmf595,
    CAST(bhdmf596 AS DOUBLE) AS bhdmf596,
    CAST(bhdmf597 AS DOUBLE) AS bhdmf597,
    CAST(bhdmf598 AS DOUBLE) AS bhdmf598,
    CAST(bhdmf599 AS DOUBLE) AS bhdmf599,
    CAST(bhdmf600 AS DOUBLE) AS bhdmf600,
    CAST(bhdmf601 AS DOUBLE) AS bhdmf601,
    CAST(bhdmf604 AS DOUBLE) AS bhdmf604,
    CAST(bhdmf605 AS DOUBLE) AS bhdmf605,
    CAST(bhdmf606 AS DOUBLE) AS bhdmf606,
    CAST(bhdmf607 AS DOUBLE) AS bhdmf607,
    CAST(bhdmf611 AS DOUBLE) AS bhdmf611,
    CAST(bhdmf612 AS DOUBLE) AS bhdmf612,
    CAST(bhdmf613 AS DOUBLE) AS bhdmf613,
    CAST(bhdmf614 AS DOUBLE) AS bhdmf614,
    CAST(bhdmf615 AS DOUBLE) AS bhdmf615,
    CAST(bhdmf616 AS DOUBLE) AS bhdmf616,
    CAST(bhdmf617 AS DOUBLE) AS bhdmf617,
    CAST(bhdmf618 AS DOUBLE) AS bhdmf618,
    CAST(bhdmf624 AS DOUBLE) AS bhdmf624,
    CAST(bhdmf625 AS DOUBLE) AS bhdmf625,
    CAST(bhdmf626 AS DOUBLE) AS bhdmf626,
    CAST(bhdmf627 AS DOUBLE) AS bhdmf627,
    CAST(bhdmf628 AS DOUBLE) AS bhdmf628,
    CAST(bhdmf629 AS DOUBLE) AS bhdmf629,
    CAST(bhdmf630 AS DOUBLE) AS bhdmf630,
    CAST(bhdmf631 AS DOUBLE) AS bhdmf631,
    CAST(bhdmf632 AS DOUBLE) AS bhdmf632,
    CAST(bhdmf633 AS DOUBLE) AS bhdmf633,
    CAST(bhdmf634 AS DOUBLE) AS bhdmf634,
    CAST(bhdmf635 AS DOUBLE) AS bhdmf635,
    CAST(bhdmf636 AS DOUBLE) AS bhdmf636,
    CAST(bhdmf639 AS DOUBLE) AS bhdmf639,
    CAST(bhdmf640 AS DOUBLE) AS bhdmf640,
    CAST(bhdmf670 AS DOUBLE) AS bhdmf670,
    CAST(bhdmf671 AS DOUBLE) AS bhdmf671,
    CAST(bhdmf672 AS DOUBLE) AS bhdmf672,
    CAST(bhdmf673 AS DOUBLE) AS bhdmf673,
    CAST(bhdmf674 AS DOUBLE) AS bhdmf674,
    CAST(bhdmf675 AS DOUBLE) AS bhdmf675,
    CAST(bhdmf676 AS DOUBLE) AS bhdmf676,
    CAST(bhdmf677 AS DOUBLE) AS bhdmf677,
    CAST(bhdmf678 AS DOUBLE) AS bhdmf678,
    CAST(bhdmf679 AS DOUBLE) AS bhdmf679,
    CAST(bhdmf680 AS DOUBLE) AS bhdmf680,
    CAST(bhdmf681 AS DOUBLE) AS bhdmf681,
    CAST(bhdmf724 AS DOUBLE) AS bhdmf724,
    CAST(bhdmg209 AS DOUBLE) AS bhdmg209,
    CAST(bhdmg210 AS DOUBLE) AS bhdmg210,
    CAST(bhdmg211 AS DOUBLE) AS bhdmg211,
    CAST(bhdmg299 AS DOUBLE) AS bhdmg299,
    CAST(bhdmg332 AS DOUBLE) AS bhdmg332,
    CAST(bhdmg333 AS DOUBLE) AS bhdmg333,
    CAST(bhdmg334 AS DOUBLE) AS bhdmg334,
    CAST(bhdmg335 AS DOUBLE) AS bhdmg335,
    CAST(bhdmg379 AS DOUBLE) AS bhdmg379,
    CAST(bhdmg380 AS DOUBLE) AS bhdmg380,
    CAST(bhdmg381 AS DOUBLE) AS bhdmg381,
    CAST(bhdmg382 AS DOUBLE) AS bhdmg382,
    CAST(bhdmg383 AS DOUBLE) AS bhdmg383,
    CAST(bhdmg384 AS DOUBLE) AS bhdmg384,
    CAST(bhdmg385 AS DOUBLE) AS bhdmg385,
    CAST(bhdmg386 AS DOUBLE) AS bhdmg386,
    CAST(bhdmg387 AS DOUBLE) AS bhdmg387,
    CAST(bhdmg388 AS DOUBLE) AS bhdmg388,
    CAST(bhdmg651 AS DOUBLE) AS bhdmg651,
    CAST(bhdmg652 AS DOUBLE) AS bhdmg652,
    CAST(bhdmhk06 AS DOUBLE) AS bhdmhk06,
    CAST(bhdmhk31 AS DOUBLE) AS bhdmhk31,
    CAST(bhdmhk32 AS DOUBLE) AS bhdmhk32,
    CAST(bhdmj451 AS DOUBLE) AS bhdmj451,
    CAST(bhdmj454 AS DOUBLE) AS bhdmj454,
    CAST(bhdmk045 AS DOUBLE) AS bhdmk045,
    CAST(bhdmk046 AS DOUBLE) AS bhdmk046,
    CAST(bhdmk047 AS DOUBLE) AS bhdmk047,
    CAST(bhdmk048 AS DOUBLE) AS bhdmk048,
    CAST(bhdmk049 AS DOUBLE) AS bhdmk049,
    CAST(bhdmk050 AS DOUBLE) AS bhdmk050,
    CAST(bhdmk051 AS DOUBLE) AS bhdmk051,
    CAST(bhdmk052 AS DOUBLE) AS bhdmk052,
    CAST(bhdmk053 AS DOUBLE) AS bhdmk053,
    CAST(bhdmk054 AS DOUBLE) AS bhdmk054,
    CAST(bhdmk055 AS DOUBLE) AS bhdmk055,
    CAST(bhdmk056 AS DOUBLE) AS bhdmk056,
    CAST(bhdmk057 AS DOUBLE) AS bhdmk057,
    CAST(bhdmk058 AS DOUBLE) AS bhdmk058,
    CAST(bhdmk059 AS DOUBLE) AS bhdmk059,
    CAST(bhdmk060 AS DOUBLE) AS bhdmk060,
    CAST(bhdmk061 AS DOUBLE) AS bhdmk061,
    CAST(bhdmk062 AS DOUBLE) AS bhdmk062,
    CAST(bhdmk063 AS DOUBLE) AS bhdmk063,
    CAST(bhdmk064 AS DOUBLE) AS bhdmk064,
    CAST(bhdmk065 AS DOUBLE) AS bhdmk065,
    CAST(bhdmk066 AS DOUBLE) AS bhdmk066,
    CAST(bhdmk067 AS DOUBLE) AS bhdmk067,
    CAST(bhdmk068 AS DOUBLE) AS bhdmk068,
    CAST(bhdmk069 AS DOUBLE) AS bhdmk069,
    CAST(bhdmk070 AS DOUBLE) AS bhdmk070,
    CAST(bhdmk071 AS DOUBLE) AS bhdmk071,
    CAST(bhdmk105 AS DOUBLE) AS bhdmk105,
    CAST(bhdmk106 AS DOUBLE) AS bhdmk106,
    CAST(bhdmk107 AS DOUBLE) AS bhdmk107,
    CAST(bhdmk108 AS DOUBLE) AS bhdmk108,
    CAST(bhdmk109 AS DOUBLE) AS bhdmk109,
    CAST(bhdmk110 AS DOUBLE) AS bhdmk110,
    CAST(bhdmk111 AS DOUBLE) AS bhdmk111,
    CAST(bhdmk112 AS DOUBLE) AS bhdmk112,
    CAST(bhdmk113 AS DOUBLE) AS bhdmk113,
    CAST(bhdmk114 AS DOUBLE) AS bhdmk114,
    CAST(bhdmk115 AS DOUBLE) AS bhdmk115,
    CAST(bhdmk116 AS DOUBLE) AS bhdmk116,
    CAST(bhdmk117 AS DOUBLE) AS bhdmk117,
    CAST(bhdmk118 AS DOUBLE) AS bhdmk118,
    CAST(bhdmk119 AS DOUBLE) AS bhdmk119,
    CAST(bhdmk130 AS DOUBLE) AS bhdmk130,
    CAST(bhdmk131 AS DOUBLE) AS bhdmk131,
    CAST(bhdmk132 AS DOUBLE) AS bhdmk132,
    CAST(bhdmk158 AS DOUBLE) AS bhdmk158,
    CAST(bhdmk159 AS DOUBLE) AS bhdmk159,
    CAST(bhdmk160 AS DOUBLE) AS bhdmk160,
    CAST(bhdmk161 AS DOUBLE) AS bhdmk161,
    CAST(bhdmk162 AS DOUBLE) AS bhdmk162,
    CAST(bhdmk166 AS DOUBLE) AS bhdmk166,
    CAST(bhdmk169 AS DOUBLE) AS bhdmk169,
    CAST(bhdmk170 AS DOUBLE) AS bhdmk170,
    CAST(bhdmk171 AS DOUBLE) AS bhdmk171,
    CAST(bhdmk172 AS DOUBLE) AS bhdmk172,
    CAST(bhdmk173 AS DOUBLE) AS bhdmk173,
    CAST(bhdmk174 AS DOUBLE) AS bhdmk174,
    CAST(bhdmk175 AS DOUBLE) AS bhdmk175,
    CAST(bhdmk176 AS DOUBLE) AS bhdmk176,
    CAST(bhdmk177 AS DOUBLE) AS bhdmk177,
    CAST(bhdmk187 AS DOUBLE) AS bhdmk187,
    CAST(bhdmk188 AS DOUBLE) AS bhdmk188,
    CAST(bhdmk189 AS DOUBLE) AS bhdmk189,
    CAST(bhdmk190 AS DOUBLE) AS bhdmk190,
    CAST(bhdmk191 AS DOUBLE) AS bhdmk191,
    CAST(bhdmk195 AS DOUBLE) AS bhdmk195,
    CAST(bhdmk196 AS DOUBLE) AS bhdmk196,
    CAST(bhdmk197 AS DOUBLE) AS bhdmk197,
    CAST(bhdmk198 AS DOUBLE) AS bhdmk198,
    CAST(bhdmk199 AS DOUBLE) AS bhdmk199,
    CAST(bhdmk200 AS DOUBLE) AS bhdmk200,
    CAST(bhdmk208 AS DOUBLE) AS bhdmk208,
    CAST(bhdmk209 AS DOUBLE) AS bhdmk209,
    CAST(bhdmk210 AS DOUBLE) AS bhdmk210,
    CAST(bhdmk211 AS DOUBLE) AS bhdmk211,
    CAST(bhdmkx57 AS DOUBLE) AS bhdmkx57,
    CAST(bhfn3360 AS DOUBLE) AS bhfn3360,
    CAST(bhfn3543 AS DOUBLE) AS bhfn3543,
    CAST(bhfn6631 AS DOUBLE) AS bhfn6631,
    CAST(bhfn6636 AS DOUBLE) AS bhfn6636,
    CAST(bhfna245 AS DOUBLE) AS bhfna245,
    CAST(bhfnk260 AS DOUBLE) AS bhfnk260,
    CAST(bhod2389 AS DOUBLE) AS bhod2389,
    CAST(bhod2604 AS DOUBLE) AS bhod2604,
    CAST(bhod3187 AS DOUBLE) AS bhod3187,
    CAST(bhod3189 AS DOUBLE) AS bhod3189,
    CAST(bhod6648 AS DOUBLE) AS bhod6648,
    CAST(bhodhk29 AS DOUBLE) AS bhodhk29,
    CAST(bhodj474 AS DOUBLE) AS bhodj474,
    CAST(bhpa0365 AS DOUBLE) AS bhpa0365,
    CAST(bhpa4340 AS DOUBLE) AS bhpa4340,
    CAST(bhpx8901 AS VARCHAR) AS bhpx8901,
    CAST(bhsp0010 AS DOUBLE) AS bhsp0010,
    CAST(bhsp0027 AS DOUBLE) AS bhsp0027,
    CAST(bhsp0087 AS DOUBLE) AS bhsp0087,
    CAST(bhsp0088 AS DOUBLE) AS bhsp0088,
    CAST(bhsp0089 AS DOUBLE) AS bhsp0089,
    CAST(bhsp0201 AS DOUBLE) AS bhsp0201,
    CAST(bhsp0202 AS DOUBLE) AS bhsp0202,
    CAST(bhsp0206 AS DOUBLE) AS bhsp0206,
    CAST(bhsp0390 AS DOUBLE) AS bhsp0390,
    CAST(bhsp0416 AS DOUBLE) AS bhsp0416,
    CAST(bhsp0447 AS DOUBLE) AS bhsp0447,
    CAST(bhsp0496 AS DOUBLE) AS bhsp0496,
    CAST(bhsp0508 AS DOUBLE) AS bhsp0508,
    CAST(bhsp0523 AS DOUBLE) AS bhsp0523,
    CAST(bhsp0530 AS DOUBLE) AS bhsp0530,
    CAST(bhsp1283 AS DOUBLE) AS bhsp1283,
    CAST(bhsp2111 AS DOUBLE) AS bhsp2111,
    CAST(bhsp2112 AS DOUBLE) AS bhsp2112,
    CAST(bhsp2122 AS DOUBLE) AS bhsp2122,
    CAST(bhsp2145 AS DOUBLE) AS bhsp2145,
    CAST(bhsp2148 AS DOUBLE) AS bhsp2148,
    CAST(bhsp2170 AS DOUBLE) AS bhsp2170,
    CAST(bhsp2309 AS DOUBLE) AS bhsp2309,
    CAST(bhsp2723 AS DOUBLE) AS bhsp2723,
    CAST(bhsp2724 AS DOUBLE) AS bhsp2724,
    CAST(bhsp2792 AS DOUBLE) AS bhsp2792,
    CAST(bhsp2794 AS DOUBLE) AS bhsp2794,
    CAST(bhsp2796 AS DOUBLE) AS bhsp2796,
    CAST(bhsp2932 AS DOUBLE) AS bhsp2932,
    CAST(bhsp3049 AS DOUBLE) AS bhsp3049,
    CAST(bhsp3066 AS DOUBLE) AS bhsp3066,
    CAST(bhsp3123 AS DOUBLE) AS bhsp3123,
    CAST(bhsp3148 AS DOUBLE) AS bhsp3148,
    CAST(bhsp3151 AS DOUBLE) AS bhsp3151,
    CAST(bhsp3152 AS DOUBLE) AS bhsp3152,
    CAST(bhsp3153 AS DOUBLE) AS bhsp3153,
    CAST(bhsp3154 AS DOUBLE) AS bhsp3154,
    CAST(bhsp3155 AS DOUBLE) AS bhsp3155,
    CAST(bhsp3156 AS DOUBLE) AS bhsp3156,
    CAST(bhsp3158 AS DOUBLE) AS bhsp3158,
    CAST(bhsp3166 AS DOUBLE) AS bhsp3166,
    CAST(bhsp3167 AS DOUBLE) AS bhsp3167,
    CAST(bhsp3210 AS DOUBLE) AS bhsp3210,
    CAST(bhsp3230 AS DOUBLE) AS bhsp3230,
    CAST(bhsp3238 AS DOUBLE) AS bhsp3238,
    CAST(bhsp3239 AS DOUBLE) AS bhsp3239,
    CAST(bhsp3247 AS DOUBLE) AS bhsp3247,
    CAST(bhsp3283 AS DOUBLE) AS bhsp3283,
    CAST(bhsp3300 AS DOUBLE) AS bhsp3300,
    CAST(bhsp3513 AS DOUBLE) AS bhsp3513,
    CAST(bhsp3523 AS DOUBLE) AS bhsp3523,
    CAST(bhsp3524 AS DOUBLE) AS bhsp3524,
    CAST(bhsp3525 AS DOUBLE) AS bhsp3525,
    CAST(bhsp3526 AS DOUBLE) AS bhsp3526,
    CAST(bhsp3527 AS DOUBLE) AS bhsp3527,
    CAST(bhsp3605 AS DOUBLE) AS bhsp3605,
    CAST(bhsp3620 AS DOUBLE) AS bhsp3620,
    CAST(bhsp3621 AS DOUBLE) AS bhsp3621,
    CAST(bhsp4000 AS DOUBLE) AS bhsp4000,
    CAST(bhsp4073 AS DOUBLE) AS bhsp4073,
    CAST(bhsp4093 AS DOUBLE) AS bhsp4093,
    CAST(bhsp4130 AS DOUBLE) AS bhsp4130,
    CAST(bhsp4250 AS DOUBLE) AS bhsp4250,
    CAST(bhsp4302 AS DOUBLE) AS bhsp4302,
    CAST(bhsp4336 AS DOUBLE) AS bhsp4336,
    CAST(bhsp4340 AS DOUBLE) AS bhsp4340,
    CAST(bhsp4778 AS DOUBLE) AS bhsp4778,
    CAST(bhsp5993 AS DOUBLE) AS bhsp5993,
    CAST(bhsp6416 AS DOUBLE) AS bhsp6416,
    CAST(bhsp6649 AS DOUBLE) AS bhsp6649,
    CAST(bhsp6796 AS DOUBLE) AS bhsp6796,
    CAST(bhsp6797 AS DOUBLE) AS bhsp6797,
    CAST(bhsp8434 AS DOUBLE) AS bhsp8434,
    CAST(bhsp8516 AS DOUBLE) AS bhsp8516,
    CAST(bhsp8517 AS DOUBLE) AS bhsp8517,
    CAST(bhsp8519 AS DOUBLE) AS bhsp8519,
    CAST(bhsp8520 AS DOUBLE) AS bhsp8520,
    CAST(bhsp8521 AS DOUBLE) AS bhsp8521,
    CAST(bhsp8522 AS DOUBLE) AS bhsp8522,
    CAST(bhsp8523 AS DOUBLE) AS bhsp8523,
    CAST(bhsp8524 AS DOUBLE) AS bhsp8524,
    CAST(bhsp8525 AS DOUBLE) AS bhsp8525,
    CAST(bhsp8526 AS DOUBLE) AS bhsp8526,
    CAST(bhsp8527 AS DOUBLE) AS bhsp8527,
    CAST(bhsp8528 AS DOUBLE) AS bhsp8528,
    CAST(bhsp8529 AS DOUBLE) AS bhsp8529,
    CAST(bhsp8530 AS DOUBLE) AS bhsp8530,
    CAST(bhsp8843 AS DOUBLE) AS bhsp8843,
    CAST(bhsp9191 AS DOUBLE) AS bhsp9191,
    CAST(bhsp9802 AS DOUBLE) AS bhsp9802,
    CAST(bhspa024 AS DOUBLE) AS bhspa024,
    CAST(bhspa130 AS DOUBLE) AS bhspa130,
    CAST(bhspa530 AS DOUBLE) AS bhspa530,
    CAST(bhspb530 AS DOUBLE) AS bhspb530,
    CAST(bhspc009 AS DOUBLE) AS bhspc009,
    CAST(bhspc159 AS DOUBLE) AS bhspc159,
    CAST(bhspc160 AS BOOLEAN) AS bhspc160,
    CAST(bhspc161 AS DOUBLE) AS bhspc161,
    CAST(bhspc252 AS DOUBLE) AS bhspc252,
    CAST(bhspc253 AS DOUBLE) AS bhspc253,
    CAST(bhspc254 AS DOUBLE) AS bhspc254,
    CAST(bhspc255 AS DOUBLE) AS bhspc255,
    CAST(bhspc256 AS DOUBLE) AS bhspc256,
    CAST(bhspc257 AS DOUBLE) AS bhspc257,
    CAST(bhspc427 AS DOUBLE) AS bhspc427,
    CAST(bhspc428 AS DOUBLE) AS bhspc428,
    CAST(bhspc447 AS DOUBLE) AS bhspc447,
    CAST(bhspc700 AS DOUBLE) AS bhspc700,
    CAST(bhspc701 AS DOUBLE) AS bhspc701,
    CAST(bhspc702 AS DOUBLE) AS bhspc702,
    CAST(bhspc884 AS DOUBLE) AS bhspc884,
    CAST(bhspf074 AS DOUBLE) AS bhspf074,
    CAST(bhspf075 AS DOUBLE) AS bhspf075,
    CAST(bhspf229 AS DOUBLE) AS bhspf229,
    CAST(bhspf819 AS DOUBLE) AS bhspf819,
    CAST(bhspf820 AS DOUBLE) AS bhspf820,
    CAST(bhspf838 AS DOUBLE) AS bhspf838,
    CAST(bhspf841 AS BOOLEAN) AS bhspf841,
    CAST(bhspf842 AS BOOLEAN) AS bhspf842,
    CAST(bhspft28 AS DOUBLE) AS bhspft28,
    CAST(bhspft42 AS BOOLEAN) AS bhspft42,
    CAST(bhspft43 AS BOOLEAN) AS bhspft43,
    CAST(bhspft44 AS BOOLEAN) AS bhspft44,
    CAST(bhspg234 AS DOUBLE) AS bhspg234,
    CAST(bhspg235 AS DOUBLE) AS bhspg235,
    CAST(bhspht69 AS DOUBLE) AS bhspht69,
    CAST(bhspht70 AS DOUBLE) AS bhspht70,
    CAST(bhspht95 AS DOUBLE) AS bhspht95,
    CAST(bhspj980 AS DOUBLE) AS bhspj980,
    CAST(bhspk141 AS DOUBLE) AS bhspk141,
    CAST(bhspky38 AS BOOLEAN) AS bhspky38,
    CAST(bhspm962 AS DOUBLE) AS bhspm962,
    CAST(bhspmz36 AS DOUBLE) AS bhspmz36,
    CAST(bhspnk60 AS BOOLEAN) AS bhspnk60,
    CAST(bhsx8901 AS VARCHAR) AS bhsx8901,
    CAST(bhtxf655 AS VARCHAR) AS bhtxf655,
    CAST(bhtxf656 AS VARCHAR) AS bhtxf656,
    CAST(bhtxf657 AS VARCHAR) AS bhtxf657,
    CAST(bhtxf658 AS VARCHAR) AS bhtxf658,
    CAST(bhtxf659 AS VARCHAR) AS bhtxf659,
    CAST(bhtxf660 AS VARCHAR) AS bhtxf660,
    CAST(bhtxg546 AS VARCHAR) AS bhtxg546,
    CAST(bhtxg551 AS VARCHAR) AS bhtxg551,
    CAST(bhtxg556 AS VARCHAR) AS bhtxg556,
    CAST(bhtxg561 AS VARCHAR) AS bhtxg561,
    CAST(bhtxg571 AS VARCHAR) AS bhtxg571,
    CAST(bhtxg576 AS BOOLEAN) AS bhtxg576,
    CAST(bhtxg581 AS VARCHAR) AS bhtxg581,
    CAST(bhtxg586 AS VARCHAR) AS bhtxg586,
    CAST(rssd4087 AS VARCHAR) AS rssd4087,
    CAST(rssd6191 AS DOUBLE) AS rssd6191,
    TRY_CAST(rssd9001 AS DOUBLE) AS rssd9001,
    CAST(rssd9005 AS VARCHAR) AS rssd9005,
    CAST(rssd9007 AS DATE) AS rssd9007,
    CAST(rssd9008 AS DATE) AS rssd9008,
    CAST(rssd9010 AS VARCHAR) AS rssd9010,
    CAST(rssd9014 AS DOUBLE) AS rssd9014,
    CAST(rssd9016 AS DOUBLE) AS rssd9016,
    CAST(rssd9017 AS VARCHAR) AS rssd9017,
    CAST(rssd9028 AS VARCHAR) AS rssd9028,
    CAST(rssd9029 AS VARCHAR) AS rssd9029,
    CAST(rssd9030 AS DOUBLE) AS rssd9030,
    CAST(rssd9031 AS DOUBLE) AS rssd9031,
    CAST(rssd9032 AS DOUBLE) AS rssd9032,
    CAST(rssd9037 AS DOUBLE) AS rssd9037,
    CAST(rssd9038 AS VARCHAR) AS rssd9038,
    CAST(rssd9039 AS DOUBLE) AS rssd9039,
    CAST(rssd9042 AS DOUBLE) AS rssd9042,
    CAST(rssd9044 AS DOUBLE) AS rssd9044,
    CAST(rssd9045 AS DOUBLE) AS rssd9045,
    CAST(rssd9046 AS DOUBLE) AS rssd9046,
    CAST(rssd9047 AS DOUBLE) AS rssd9047,
    CAST(rssd9048 AS DOUBLE) AS rssd9048,
    CAST(rssd9049 AS DOUBLE) AS rssd9049,
    CAST(rssd9050 AS DOUBLE) AS rssd9050,
    CAST(rssd9052 AS DOUBLE) AS rssd9052,
    CAST(rssd9053 AS DOUBLE) AS rssd9053,
    CAST(rssd9054 AS DOUBLE) AS rssd9054,
    CAST(rssd9055 AS DOUBLE) AS rssd9055,
    CAST(rssd9056 AS DOUBLE) AS rssd9056,
    CAST(rssd9059 AS DOUBLE) AS rssd9059,
    CAST(rssd9060 AS DOUBLE) AS rssd9060,
    CAST(rssd9061 AS DOUBLE) AS rssd9061,
    CAST(rssd9101 AS VARCHAR) AS rssd9101,
    CAST(rssd9130 AS VARCHAR) AS rssd9130,
    CAST(rssd9132 AS DOUBLE) AS rssd9132,
    CAST(rssd9138 AS DOUBLE) AS rssd9138,
    CAST(rssd9146 AS DOUBLE) AS rssd9146,
    CAST(rssd9150 AS DOUBLE) AS rssd9150,
    CAST(rssd9161 AS VARCHAR) AS rssd9161,
    CAST(rssd9170 AS DOUBLE) AS rssd9170,
    CAST(rssd9192 AS VARCHAR) AS rssd9192,
    CAST(rssd9198 AS DOUBLE) AS rssd9198,
    CAST(rssd9200 AS VARCHAR) AS rssd9200,
    CAST(rssd9210 AS DOUBLE) AS rssd9210,
    CAST(rssd9213 AS DOUBLE) AS rssd9213,
    CAST(rssd9216 AS DOUBLE) AS rssd9216,
    CAST(rssd9220 AS VARCHAR) AS rssd9220,
    CAST(rssd9320 AS DOUBLE) AS rssd9320,
    CAST(rssd9374 AS DOUBLE) AS rssd9374,
    CAST(rssd9375 AS DOUBLE) AS rssd9375,
    CAST(rssd9421 AS DOUBLE) AS rssd9421,
    CAST(rssd9422 AS DOUBLE) AS rssd9422,
    CAST(rssd9424 AS DOUBLE) AS rssd9424,
    CAST(rssd9425 AS DOUBLE) AS rssd9425,
    CAST(rssd9579 AS DOUBLE) AS rssd9579,
    CAST(rssd9950 AS DATE) AS rssd9950,
    CAST(rssd9955 AS DOUBLE) AS rssd9955,
    TRY_CAST(strftime(CAST(rssd9999 AS DATE), '%Y%m%d') AS DOUBLE) AS rssd9999,
    CAST(texc3573 AS BOOLEAN) AS texc3573,
    CAST(texc3575 AS BOOLEAN) AS texc3575,
    CAST(texc6373 AS DOUBLE) AS texc6373,
    CAST(texc6561 AS BOOLEAN) AS texc6561,
    CAST(texc6562 AS BOOLEAN) AS texc6562,
    CAST(texc6568 AS BOOLEAN) AS texc6568,
    CAST(texc6586 AS BOOLEAN) AS texc6586,
    CAST(texc6995 AS BOOLEAN) AS texc6995,
    CAST(texc6996 AS BOOLEAN) AS texc6996,
    CAST(texc6997 AS BOOLEAN) AS texc6997,
    CAST(texc6998 AS BOOLEAN) AS texc6998,
    CAST(texc8520 AS DOUBLE) AS texc8520,
    CAST(texc8521 AS BOOLEAN) AS texc8521,
    CAST(texc8522 AS BOOLEAN) AS texc8522,
    CAST(texc8523 AS DOUBLE) AS texc8523,
    CAST(texc8524 AS DOUBLE) AS texc8524,
    CAST(texc8525 AS BOOLEAN) AS texc8525,
    CAST(texc8557 AS DOUBLE) AS texc8557,
    CAST(texc8558 AS DOUBLE) AS texc8558,
    CAST(texc8559 AS DOUBLE) AS texc8559,
    CAST(texc8562 AS DOUBLE) AS texc8562,
    CAST(texc8563 AS DOUBLE) AS texc8563,
    CAST(texc8564 AS DOUBLE) AS texc8564,
    CAST(texc8565 AS DOUBLE) AS texc8565,
    CAST(texc8566 AS DOUBLE) AS texc8566,
    CAST(texc8567 AS DOUBLE) AS texc8567,
    CAST(text3571 AS VARCHAR) AS text3571,
    CAST(text3573 AS VARCHAR) AS text3573,
    CAST(text3575 AS VARCHAR) AS text3575,
    CAST(text4769 AS BOOLEAN) AS text4769,
    CAST(text5351 AS VARCHAR) AS text5351,
    CAST(text5352 AS VARCHAR) AS text5352,
    CAST(text5353 AS VARCHAR) AS text5353,
    CAST(text5354 AS VARCHAR) AS text5354,
    CAST(text5355 AS VARCHAR) AS text5355,
    CAST(text5356 AS VARCHAR) AS text5356,
    CAST(text5357 AS VARCHAR) AS text5357,
    CAST(text5358 AS VARCHAR) AS text5358,
    CAST(text5359 AS VARCHAR) AS text5359,
    CAST(text5360 AS VARCHAR) AS text5360,
    CAST(text5485 AS VARCHAR) AS text5485,
    CAST(text5486 AS VARCHAR) AS text5486,
    CAST(text5487 AS VARCHAR) AS text5487,
    CAST(text5488 AS VARCHAR) AS text5488,
    CAST(text5489 AS VARCHAR) AS text5489,
    CAST(text5523 AS BOOLEAN) AS text5523,
    CAST(text6373 AS VARCHAR) AS text6373,
    CAST(text6561 AS VARCHAR) AS text6561,
    CAST(text6562 AS VARCHAR) AS text6562,
    CAST(text6568 AS VARCHAR) AS text6568,
    CAST(text6586 AS VARCHAR) AS text6586,
    CAST(text6995 AS BOOLEAN) AS text6995,
    CAST(text6996 AS BOOLEAN) AS text6996,
    CAST(text6997 AS BOOLEAN) AS text6997,
    CAST(text6998 AS BOOLEAN) AS text6998,
    CAST(text8520 AS VARCHAR) AS text8520,
    CAST(text8521 AS VARCHAR) AS text8521,
    CAST(text8522 AS VARCHAR) AS text8522,
    CAST(text8523 AS VARCHAR) AS text8523,
    CAST(text8524 AS VARCHAR) AS text8524,
    CAST(text8525 AS VARCHAR) AS text8525,
    CAST(text8526 AS VARCHAR) AS text8526,
    CAST(text8527 AS VARCHAR) AS text8527,
    CAST(text8528 AS VARCHAR) AS text8528,
    CAST(text8529 AS VARCHAR) AS text8529,
    CAST(text8530 AS VARCHAR) AS text8530,
    CAST(text8557 AS VARCHAR) AS text8557,
    CAST(text8558 AS VARCHAR) AS text8558,
    CAST(text8559 AS VARCHAR) AS text8559,
    CAST(text8562 AS VARCHAR) AS text8562,
    CAST(text8563 AS VARCHAR) AS text8563,
    CAST(text8564 AS VARCHAR) AS text8564,
    CAST(text8565 AS VARCHAR) AS text8565,
    CAST(text8566 AS VARCHAR) AS text8566,
    CAST(text8567 AS VARCHAR) AS text8567,
    CAST(textb027 AS VARCHAR) AS textb027,
    CAST(textb028 AS VARCHAR) AS textb028,
    CAST(textb029 AS VARCHAR) AS textb029,
    CAST(textb030 AS VARCHAR) AS textb030,
    CAST(textb031 AS VARCHAR) AS textb031,
    CAST(textb032 AS VARCHAR) AS textb032,
    CAST(textb033 AS VARCHAR) AS textb033,
    CAST(textb034 AS VARCHAR) AS textb034,
    CAST(textb035 AS VARCHAR) AS textb035,
    CAST(textb036 AS VARCHAR) AS textb036,
    CAST(textb037 AS VARCHAR) AS textb037,
    CAST(textb038 AS VARCHAR) AS textb038,
    CAST(textb039 AS VARCHAR) AS textb039,
    CAST(textb040 AS VARCHAR) AS textb040,
    CAST(textb041 AS VARCHAR) AS textb041,
    CAST(textb042 AS VARCHAR) AS textb042,
    CAST(textb043 AS VARCHAR) AS textb043,
    CAST(textb044 AS VARCHAR) AS textb044,
    CAST(textb045 AS VARCHAR) AS textb045,
    CAST(textb046 AS VARCHAR) AS textb046,
    CAST(textb047 AS VARCHAR) AS textb047,
    CAST(textb048 AS VARCHAR) AS textb048,
    CAST(textb049 AS VARCHAR) AS textb049,
    CAST(textb050 AS VARCHAR) AS textb050,
    CAST(textb051 AS VARCHAR) AS textb051,
    CAST(textb052 AS VARCHAR) AS textb052,
    CAST(textb053 AS VARCHAR) AS textb053,
    CAST(textb054 AS BOOLEAN) AS textb054,
    CAST(textb055 AS BOOLEAN) AS textb055,
    CAST(textb056 AS VARCHAR) AS textb056,
    CAST(textc231 AS VARCHAR) AS textc231,
    CAST(textc490 AS BOOLEAN) AS textc490,
    CAST(textc497 AS VARCHAR) AS textc497,
    CAST(textc703 AS VARCHAR) AS textc703,
    CAST(textc708 AS VARCHAR) AS textc708,
    CAST(textc714 AS VARCHAR) AS textc714,
    CAST(textc715 AS VARCHAR) AS textc715,
    CAST(textft29 AS BOOLEAN) AS textft29,
    CAST(textft31 AS VARCHAR) AS textft31,
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
                let bhbc3368 = f("bhbc3368");
                let bhbc3402 = f("bhbc3402");
                let bhbc3516 = f("bhbc3516");
                let bhbc3519 = f("bhbc3519");
                let bhbc4070 = f("bhbc4070");
                let bhbc4073 = f("bhbc4073");
                let bhbc4074 = f("bhbc4074");
                let bhbc4079 = f("bhbc4079");
                let bhbc4091 = f("bhbc4091");
                let bhbc4093 = f("bhbc4093");
                let bhbc4094 = f("bhbc4094");
                let bhbc4107 = f("bhbc4107");
                let bhbc4135 = f("bhbc4135");
                let bhbc4218 = f("bhbc4218");
                let bhbc4230 = f("bhbc4230");
                let bhbc4301 = f("bhbc4301");
                let bhbc4302 = f("bhbc4302");
                let bhbc4320 = f("bhbc4320");
                let bhbc4340 = f("bhbc4340");
                let bhbc4421 = f("bhbc4421");
                let bhbc4475 = f("bhbc4475");
                let bhbc4484 = f("bhbc4484");
                let bhbc4519 = f("bhbc4519");
                let bhbc6061 = f("bhbc6061");
                let bhbca220 = f("bhbca220");
                let bhbcb490 = f("bhbcb490");
                let bhbcb491 = f("bhbcb491");
                let bhbcb493 = f("bhbcb493");
                let bhbcb494 = f("bhbcb494");
                let bhbcc216 = f("bhbcc216");
                let bhbcjj33 = f("bhbcjj33");
                let bhc00010 = f("bhc00010");
                let bhc00390 = f("bhc00390");
                let bhc01350 = f("bhc01350");
                let bhc01754 = f("bhc01754");
                let bhc01773 = f("bhc01773");
                let bhc02122 = f("bhc02122");
                let bhc02170 = f("bhc02170");
                let bhc03411 = f("bhc03411");
                let bhc03429 = f("bhc03429");
                let bhc03433 = f("bhc03433");
                let bhc03545 = f("bhc03545");
                let bhc05369 = f("bhc05369");
                let bhc06551 = f("bhc06551");
                let bhc06563 = f("bhc06563");
                let bhc06566 = f("bhc06566");
                let bhc06570 = f("bhc06570");
                let bhc06572 = f("bhc06572");
                let bhc06574 = f("bhc06574");
                let bhc06575 = f("bhc06575");
                let bhc06598 = f("bhc06598");
                let bhc06601 = b("bhc06601");
                let bhc06602 = f("bhc06602");
                let bhc06603 = f("bhc06603");
                let bhc0a167 = f("bhc0a167");
                let bhc0a250 = f("bhc0a250");
                let bhc0b528 = f("bhc0b528");
                let bhc0b546 = f("bhc0b546");
                let bhc0b639 = f("bhc0b639");
                let bhc0b675 = f("bhc0b675");
                let bhc0b681 = f("bhc0b681");
                let bhc0c225 = f("bhc0c225");
                let bhc0g591 = f("bhc0g591");
                let bhc20010 = f("bhc20010");
                let bhc20390 = f("bhc20390");
                let bhc21350 = f("bhc21350");
                let bhc21754 = f("bhc21754");
                let bhc21773 = f("bhc21773");
                let bhc22122 = f("bhc22122");
                let bhc22170 = f("bhc22170");
                let bhc23411 = f("bhc23411");
                let bhc23429 = f("bhc23429");
                let bhc23433 = f("bhc23433");
                let bhc23545 = f("bhc23545");
                let bhc25369 = f("bhc25369");
                let bhc26551 = f("bhc26551");
                let bhc26563 = f("bhc26563");
                let bhc26566 = f("bhc26566");
                let bhc26570 = f("bhc26570");
                let bhc26572 = f("bhc26572");
                let bhc26574 = f("bhc26574");
                let bhc26575 = f("bhc26575");
                let bhc26598 = f("bhc26598");
                let bhc26601 = f("bhc26601");
                let bhc26602 = f("bhc26602");
                let bhc26603 = f("bhc26603");
                let bhc2a167 = f("bhc2a167");
                let bhc2a250 = f("bhc2a250");
                let bhc2b528 = f("bhc2b528");
                let bhc2b546 = f("bhc2b546");
                let bhc2b639 = f("bhc2b639");
                let bhc2b675 = f("bhc2b675");
                let bhc2b681 = f("bhc2b681");
                let bhc2c225 = f("bhc2c225");
                let bhc2g591 = f("bhc2g591");
                let bhc50390 = f("bhc50390");
                let bhc51350 = b("bhc51350");
                let bhc51754 = f("bhc51754");
                let bhc51773 = f("bhc51773");
                let bhc52122 = f("bhc52122");
                let bhc52170 = f("bhc52170");
                let bhc53411 = f("bhc53411");
                let bhc53433 = f("bhc53433");
                let bhc53545 = f("bhc53545");
                let bhc55369 = f("bhc55369");
                let bhc56551 = f("bhc56551");
                let bhc56563 = f("bhc56563");
                let bhc56566 = f("bhc56566");
                let bhc56570 = f("bhc56570");
                let bhc56572 = f("bhc56572");
                let bhc56574 = f("bhc56574");
                let bhc56575 = f("bhc56575");
                let bhc56598 = f("bhc56598");
                let bhc56602 = f("bhc56602");
                let bhc56603 = f("bhc56603");
                let bhc5a167 = f("bhc5a167");
                let bhc5a250 = f("bhc5a250");
                let bhc5b528 = f("bhc5b528");
                let bhc5b546 = f("bhc5b546");
                let bhc5b639 = f("bhc5b639");
                let bhc5b675 = f("bhc5b675");
                let bhc5b681 = f("bhc5b681");
                let bhc5g591 = f("bhc5g591");
                let bhc90010 = f("bhc90010");
                let bhc90390 = f("bhc90390");
                let bhc91350 = f("bhc91350");
                let bhc91727 = f("bhc91727");
                let bhc91754 = f("bhc91754");
                let bhc91773 = f("bhc91773");
                let bhc92122 = f("bhc92122");
                let bhc92170 = f("bhc92170");
                let bhc93411 = f("bhc93411");
                let bhc93429 = f("bhc93429");
                let bhc93433 = f("bhc93433");
                let bhc93545 = f("bhc93545");
                let bhc95369 = f("bhc95369");
                let bhc96551 = f("bhc96551");
                let bhc96563 = f("bhc96563");
                let bhc96566 = f("bhc96566");
                let bhc96570 = f("bhc96570");
                let bhc96572 = f("bhc96572");
                let bhc96574 = f("bhc96574");
                let bhc96575 = f("bhc96575");
                let bhc96598 = f("bhc96598");
                let bhc96602 = f("bhc96602");
                let bhc96603 = f("bhc96603");
                let bhc9a250 = f("bhc9a250");
                let bhc9b528 = f("bhc9b528");
                let bhc9b541 = f("bhc9b541");
                let bhc9b546 = f("bhc9b546");
                let bhc9b639 = f("bhc9b639");
                let bhc9b675 = f("bhc9b675");
                let bhc9b681 = f("bhc9b681");
                let bhc9c225 = f("bhc9c225");
                let bhc9g591 = f("bhc9g591");
                let bhca2170 = f("bhca2170");
                let bhca3792 = f("bhca3792");
                let bhca5310 = f("bhca5310");
                let bhca5311 = f("bhca5311");
                let bhca7204 = f("bhca7204");
                let bhca7205 = f("bhca7205");
                let bhca7206 = f("bhca7206");
                let bhca8274 = f("bhca8274");
                let bhcaa223 = f("bhcaa223");
                let bhcaa224 = f("bhcaa224");
                let bhcab530 = f("bhcab530");
                let bhcab596 = f("bhcab596");
                let bhcah036 = f("bhcah036");
                let bhcah311 = f("bhcah311");
                let bhcah312 = b("bhcah312");
                let bhcah313 = f("bhcah313");
                let bhcah314 = f("bhcah314");
                let bhcajj29 = f("bhcajj29");
                let bhcakw00 = f("bhcakw00");
                let bhcakw03 = f("bhcakw03");
                let bhcakx77 = f("bhcakx77");
                let bhcakx78 = f("bhcakx78");
                let bhcakx79 = f("bhcakx79");
                let bhcakx80 = f("bhcakx80");
                let bhcakx81 = f("bhcakx81");
                let bhcakx82 = f("bhcakx82");
                let bhcakx83 = f("bhcakx83");
                let bhcalb58 = f("bhcalb58");
                let bhcalb59 = f("bhcalb59");
                let bhcalb60 = f("bhcalb60");
                let bhcalb61 = f("bhcalb61");
                let bhcale74 = f("bhcale74");
                let bhcale85 = f("bhcale85");
                let bhcale86 = f("bhcale86");
                let bhcale87 = f("bhcale87");
                let bhcale88 = f("bhcale88");
                let bhcale89 = f("bhcale89");
                let bhcale90 = f("bhcale90");
                let bhcale91 = f("bhcale91");
                let bhcale92 = f("bhcale92");
                let bhcalf21 = f("bhcalf21");
                let bhcalf22 = f("bhcalf22");
                let bhcalf23 = f("bhcalf23");
                let bhcalf24 = f("bhcalf24");
                let bhcalf25 = f("bhcalf25");
                let bhcalf27 = f("bhcalf27");
                let bhcalf28 = f("bhcalf28");
                let bhcamk66 = f("bhcamk66");
                let bhcamk76 = f("bhcamk76");
                let bhcamk77 = f("bhcamk77");
                let bhcamk78 = f("bhcamk78");
                let bhcanc99 = f("bhcanc99");
                let bhcap742 = f("bhcap742");
                let bhcap793 = f("bhcap793");
                let bhcap838 = f("bhcap838");
                let bhcap839 = f("bhcap839");
                let bhcap840 = f("bhcap840");
                let bhcap841 = f("bhcap841");
                let bhcap842 = f("bhcap842");
                let bhcap843 = f("bhcap843");
                let bhcap844 = f("bhcap844");
                let bhcap845 = f("bhcap845");
                let bhcap846 = f("bhcap846");
                let bhcap847 = f("bhcap847");
                let bhcap848 = f("bhcap848");
                let bhcap849 = f("bhcap849");
                let bhcap850 = f("bhcap850");
                let bhcap851 = f("bhcap851");
                let bhcap852 = f("bhcap852");
                let bhcap853 = f("bhcap853");
                let bhcap854 = f("bhcap854");
                let bhcap855 = f("bhcap855");
                let bhcap856 = f("bhcap856");
                let bhcap857 = f("bhcap857");
                let bhcap858 = f("bhcap858");
                let bhcap859 = f("bhcap859");
                let bhcap860 = f("bhcap860");
                let bhcap861 = f("bhcap861");
                let bhcap862 = f("bhcap862");
                let bhcap863 = f("bhcap863");
                let bhcap864 = f("bhcap864");
                let bhcap865 = f("bhcap865");
                let bhcap866 = f("bhcap866");
                let bhcap867 = f("bhcap867");
                let bhcap868 = f("bhcap868");
                let bhcap870 = f("bhcap870");
                let bhcap872 = f("bhcap872");
                let bhcap875 = f("bhcap875");
                let bhcaq257 = f("bhcaq257");
                let bhcaq258 = f("bhcaq258");
                let bhcas540 = f("bhcas540");
                let bhcb2210 = f("bhcb2210");
                let bhcb2389 = f("bhcb2389");
                let bhcb2604 = f("bhcb2604");
                let bhcb3187 = f("bhcb3187");
                let bhcb6648 = f("bhcb6648");
                let bhcbhk29 = f("bhcbhk29");
                let bhcbj474 = f("bhcbj474");
                let bhce0010 = f("bhce0010");
                let bhce1727 = f("bhce1727");
                let bhce1754 = f("bhce1754");
                let bhce1773 = f("bhce1773");
                let bhce2170 = f("bhce2170");
                let bhce3123 = f("bhce3123");
                let bhce3411 = f("bhce3411");
                let bhce3429 = f("bhce3429");
                let bhce3433 = f("bhce3433");
                let bhce3545 = f("bhce3545");
                let bhce5369 = f("bhce5369");
                let bhce6566 = f("bhce6566");
                let bhce6570 = f("bhce6570");
                let bhce6572 = f("bhce6572");
                let bhcea167 = f("bhcea167");
                let bhcea250 = f("bhcea250");
                let bhceb528 = f("bhceb528");
                let bhceb541 = f("bhceb541");
                let bhceb546 = f("bhceb546");
                let bhceb639 = f("bhceb639");
                let bhceb675 = f("bhceb675");
                let bhceb681 = f("bhceb681");
                let bhceg591 = f("bhceg591");
                let bhcm3531 = f("bhcm3531");
                let bhcm3532 = f("bhcm3532");
                let bhcm3533 = f("bhcm3533");
                let bhcm3534 = f("bhcm3534");
                let bhcm3535 = f("bhcm3535");
                let bhcm3536 = f("bhcm3536");
                let bhcm3537 = f("bhcm3537");
                let bhcm3541 = f("bhcm3541");
                let bhcm3543 = f("bhcm3543");
                let bhcp0010 = f("bhcp0010");
                let bhcp0087 = f("bhcp0087");
                let bhcp0201 = f("bhcp0201");
                let bhcp0202 = f("bhcp0202");
                let bhcp0203 = f("bhcp0203");
                let bhcp0204 = f("bhcp0204");
                let bhcp0205 = f("bhcp0205");
                let bhcp0206 = f("bhcp0206");
                let bhcp0207 = f("bhcp0207");
                let bhcp0208 = f("bhcp0208");
                let bhcp0209 = f("bhcp0209");
                let bhcp0210 = f("bhcp0210");
                let bhcp0277 = f("bhcp0277");
                let bhcp0279 = f("bhcp0279");
                let bhcp0362 = f("bhcp0362");
                let bhcp0363 = f("bhcp0363");
                let bhcp0364 = f("bhcp0364");
                let bhcp0365 = f("bhcp0365");
                let bhcp0368 = f("bhcp0368");
                let bhcp0400 = f("bhcp0400");
                let bhcp0416 = f("bhcp0416");
                let bhcp0447 = f("bhcp0447");
                let bhcp0467 = f("bhcp0467");
                let bhcp0496 = f("bhcp0496");
                let bhcp0508 = f("bhcp0508");
                let bhcp0512 = f("bhcp0512");
                let bhcp0515 = f("bhcp0515");
                let bhcp0518 = f("bhcp0518");
                let bhcp0520 = f("bhcp0520");
                let bhcp0522 = f("bhcp0522");
                let bhcp0533 = f("bhcp0533");
                let bhcp0534 = f("bhcp0534");
                let bhcp0536 = f("bhcp0536");
                let bhcp0537 = f("bhcp0537");
                let bhcp0538 = f("bhcp0538");
                let bhcp0539 = f("bhcp0539");
                let bhcp0540 = f("bhcp0540");
                let bhcp0541 = f("bhcp0541");
                let bhcp0542 = f("bhcp0542");
                let bhcp0543 = f("bhcp0543");
                let bhcp1273 = f("bhcp1273");
                let bhcp1274 = f("bhcp1274");
                let bhcp1275 = f("bhcp1275");
                let bhcp1276 = f("bhcp1276");
                let bhcp1277 = f("bhcp1277");
                let bhcp1278 = f("bhcp1278");
                let bhcp1279 = f("bhcp1279");
                let bhcp1299 = f("bhcp1299");
                let bhcp1403 = f("bhcp1403");
                let bhcp1407 = f("bhcp1407");
                let bhcp1616 = f("bhcp1616");
                let bhcp2123 = f("bhcp2123");
                let bhcp2125 = f("bhcp2125");
                let bhcp2145 = f("bhcp2145");
                let bhcp2160 = f("bhcp2160");
                let bhcp2165 = f("bhcp2165");
                let bhcp2170 = f("bhcp2170");
                let bhcp2200 = f("bhcp2200");
                let bhcp2309 = f("bhcp2309");
                let bhcp2332 = f("bhcp2332");
                let bhcp2792 = f("bhcp2792");
                let bhcp2793 = f("bhcp2793");
                let bhcp2794 = f("bhcp2794");
                let bhcp2796 = f("bhcp2796");
                let bhcp2831 = f("bhcp2831");
                let bhcp2930 = f("bhcp2930");
                let bhcp3123 = f("bhcp3123");
                let bhcp3128 = f("bhcp3128");
                let bhcp3147 = f("bhcp3147");
                let bhcp3152 = f("bhcp3152");
                let bhcp3153 = f("bhcp3153");
                let bhcp3156 = f("bhcp3156");
                let bhcp3163 = f("bhcp3163");
                let bhcp3164 = f("bhcp3164");
                let bhcp3165 = f("bhcp3165");
                let bhcp3210 = f("bhcp3210");
                let bhcp3230 = f("bhcp3230");
                let bhcp3238 = f("bhcp3238");
                let bhcp3239 = f("bhcp3239");
                let bhcp3240 = f("bhcp3240");
                let bhcp3247 = f("bhcp3247");
                let bhcp3283 = f("bhcp3283");
                let bhcp3290 = f("bhcp3290");
                let bhcp3293 = f("bhcp3293");
                let bhcp3298 = f("bhcp3298");
                let bhcp3300 = f("bhcp3300");
                let bhcp3409 = f("bhcp3409");
                let bhcp3513 = f("bhcp3513");
                let bhcp3602 = f("bhcp3602");
                let bhcp3603 = f("bhcp3603");
                let bhcp3604 = f("bhcp3604");
                let bhcp3605 = f("bhcp3605");
                let bhcp3606 = f("bhcp3606");
                let bhcp3607 = f("bhcp3607");
                let bhcp3609 = f("bhcp3609");
                let bhcp3611 = f("bhcp3611");
                let bhcp3612 = f("bhcp3612");
                let bhcp3613 = f("bhcp3613");
                let bhcp3614 = f("bhcp3614");
                let bhcp3615 = f("bhcp3615");
                let bhcp3616 = f("bhcp3616");
                let bhcp3617 = f("bhcp3617");
                let bhcp3618 = f("bhcp3618");
                let bhcp3619 = f("bhcp3619");
                let bhcp4000 = f("bhcp4000");
                let bhcp4062 = f("bhcp4062");
                let bhcp4073 = f("bhcp4073");
                let bhcp4091 = f("bhcp4091");
                let bhcp4130 = f("bhcp4130");
                let bhcp4135 = f("bhcp4135");
                let bhcp4230 = f("bhcp4230");
                let bhcp4243 = f("bhcp4243");
                let bhcp4250 = f("bhcp4250");
                let bhcp4302 = f("bhcp4302");
                let bhcp4320 = f("bhcp4320");
                let bhcp4336 = f("bhcp4336");
                let bhcp4340 = f("bhcp4340");
                let bhcp4485 = f("bhcp4485");
                let bhcp4605 = f("bhcp4605");
                let bhcp4635 = f("bhcp4635");
                let bhcp4647 = f("bhcp4647");
                let bhcp4778 = f("bhcp4778");
                let bhcp5485 = f("bhcp5485");
                let bhcp5486 = f("bhcp5486");
                let bhcp5487 = f("bhcp5487");
                let bhcp5488 = f("bhcp5488");
                let bhcp5489 = f("bhcp5489");
                let bhcp5993 = f("bhcp5993");
                let bhcp6552 = f("bhcp6552");
                let bhcp6567 = f("bhcp6567");
                let bhcp6571 = f("bhcp6571");
                let bhcp6573 = f("bhcp6573");
                let bhcp6588 = f("bhcp6588");
                let bhcp6589 = f("bhcp6589");
                let bhcp6590 = f("bhcp6590");
                let bhcp6591 = f("bhcp6591");
                let bhcp6592 = f("bhcp6592");
                let bhcp6596 = f("bhcp6596");
                let bhcp6600 = f("bhcp6600");
                let bhcp6604 = f("bhcp6604");
                let bhcp6607 = f("bhcp6607");
                let bhcp6619 = f("bhcp6619");
                let bhcp6649 = f("bhcp6649");
                let bhcp6741 = f("bhcp6741");
                let bhcp6742 = f("bhcp6742");
                let bhcp6743 = f("bhcp6743");
                let bhcp6744 = f("bhcp6744");
                let bhcp6758 = f("bhcp6758");
                let bhcp6773 = f("bhcp6773");
                let bhcp6775 = f("bhcp6775");
                let bhcp6791 = f("bhcp6791");
                let bhcp6792 = f("bhcp6792");
                let bhcp6793 = f("bhcp6793");
                let bhcp6794 = f("bhcp6794");
                let bhcp6795 = f("bhcp6795");
                let bhcp8434 = f("bhcp8434");
                let bhcp8516 = f("bhcp8516");
                let bhcp8517 = f("bhcp8517");
                let bhcp8518 = f("bhcp8518");
                let bhcp8843 = f("bhcp8843");
                let bhcp9191 = f("bhcp9191");
                let bhcp9802 = f("bhcp9802");
                let bhcpa130 = f("bhcpa130");
                let bhcpb530 = f("bhcpb530");
                let bhcpc254 = f("bhcpc254");
                let bhcpc255 = f("bhcpc255");
                let bhcpc427 = f("bhcpc427");
                let bhcpc428 = f("bhcpc428");
                let bhcpc447 = f("bhcpc447");
                let bhcpf229 = f("bhcpf229");
                let bhcpf737 = f("bhcpf737");
                let bhcpf817 = f("bhcpf817");
                let bhcpf818 = f("bhcpf818");
                let bhcpf819 = f("bhcpf819");
                let bhcpf820 = f("bhcpf820");
                let bhcpf838 = f("bhcpf838");
                let bhcpf841 = b("bhcpf841");
                let bhcpf842 = b("bhcpf842");
                let bhcpft28 = f("bhcpft28");
                let bhcphk02 = f("bhcphk02");
                let bhcpht69 = f("bhcpht69");
                let bhcpht70 = f("bhcpht70");
                let bhcphu25 = f("bhcphu25");
                let bhcphu26 = f("bhcphu26");
                let bhcpj980 = f("bhcpj980");
                let bhcpja22 = f("bhcpja22");
                let bhcpjj33 = f("bhcpjj33");
                let bhcpk297 = f("bhcpk297");
                let bhcpky38 = b("bhcpky38");
                let bhcpm962 = f("bhcpm962");
                let bhct0426 = f("bhct0426");
                let bhct1754 = f("bhct1754");
                let bhct1773 = f("bhct1773");
                let bhct2143 = f("bhct2143");
                let bhct2150 = f("bhct2150");
                let bhct2160 = f("bhct2160");
                let bhct2170 = f("bhct2170");
                let bhct2750 = f("bhct2750");
                let bhct3123 = f("bhct3123");
                let bhct3190 = f("bhct3190");
                let bhct3210 = f("bhct3210");
                let bhct3247 = f("bhct3247");
                let bhct3368 = f("bhct3368");
                let bhct3411 = f("bhct3411");
                let bhct3433 = f("bhct3433");
                let bhct3543 = f("bhct3543");
                let bhct3545 = f("bhct3545");
                let bhct3547 = f("bhct3547");
                let bhct3548 = f("bhct3548");
                let bhct4230 = f("bhct4230");
                let bhct4340 = f("bhct4340");
                let bhct4605 = f("bhct4605");
                let bhct5369 = f("bhct5369");
                let bhct5610 = f("bhct5610");
                let bhct6570 = f("bhct6570");
                let bhcta250 = f("bhcta250");
                let bhctb528 = f("bhctb528");
                let bhctb590 = f("bhctb590");
                let bhctb591 = f("bhctb591");
                let bhcw3792 = f("bhcw3792");
                let bhcw5310 = f("bhcw5310");
                let bhcw5311 = f("bhcw5311");
                let bhcw7205 = f("bhcw7205");
                let bhcw7206 = f("bhcw7206");
                let bhcwa223 = f("bhcwa223");
                let bhcwh311 = f("bhcwh311");
                let bhcwkx78 = b("bhcwkx78");
                let bhcwkx83 = b("bhcwkx83");
                let bhcwle85 = f("bhcwle85");
                let bhcwle86 = f("bhcwle86");
                let bhcwle87 = f("bhcwle87");
                let bhcwlf23 = f("bhcwlf23");
                let bhcwlf24 = f("bhcwlf24");
                let bhcwlf25 = f("bhcwlf25");
                let bhcwmk66 = f("bhcwmk66");
                let bhcwp793 = f("bhcwp793");
                let bhcwp851 = f("bhcwp851");
                let bhcwp852 = f("bhcwp852");
                let bhcwp853 = f("bhcwp853");
                let bhcwp854 = f("bhcwp854");
                let bhcwp855 = f("bhcwp855");
                let bhcwp856 = f("bhcwp856");
                let bhcwp857 = f("bhcwp857");
                let bhcwp858 = f("bhcwp858");
                let bhcwp859 = f("bhcwp859");
                let bhcwp870 = f("bhcwp870");
                let bhcx1754 = f("bhcx1754");
                let bhcx1773 = f("bhcx1773");
                let bhcx3123 = f("bhcx3123");
                let bhcx3210 = f("bhcx3210");
                let bhcx3368 = f("bhcx3368");
                let bhcx3545 = f("bhcx3545");
                let bhcy1773 = f("bhcy1773");
                let bhcy3123 = f("bhcy3123");
                let bhcyja36 = f("bhcyja36");
                let bhdm1288 = f("bhdm1288");
                let bhdm1410 = f("bhdm1410");
                let bhdm1415 = f("bhdm1415");
                let bhdm1420 = f("bhdm1420");
                let bhdm1460 = f("bhdm1460");
                let bhdm1480 = f("bhdm1480");
                let bhdm1545 = f("bhdm1545");
                let bhdm1564 = f("bhdm1564");
                let bhdm1590 = f("bhdm1590");
                let bhdm1635 = f("bhdm1635");
                let bhdm1755 = f("bhdm1755");
                let bhdm1766 = f("bhdm1766");
                let bhdm1797 = f("bhdm1797");
                let bhdm1975 = f("bhdm1975");
                let bhdm2081 = f("bhdm2081");
                let bhdm2122 = f("bhdm2122");
                let bhdm2123 = f("bhdm2123");
                let bhdm2165 = f("bhdm2165");
                let bhdm3386 = f("bhdm3386");
                let bhdm3387 = f("bhdm3387");
                let bhdm3465 = f("bhdm3465");
                let bhdm3466 = f("bhdm3466");
                let bhdm3516 = f("bhdm3516");
                let bhdm3545 = f("bhdm3545");
                let bhdm3546 = f("bhdm3546");
                let bhdm3547 = f("bhdm3547");
                let bhdm3548 = f("bhdm3548");
                let bhdm5367 = f("bhdm5367");
                let bhdm5368 = f("bhdm5368");
                let bhdm6631 = f("bhdm6631");
                let bhdm6636 = f("bhdm6636");
                let bhdma164 = f("bhdma164");
                let bhdma242 = f("bhdma242");
                let bhdma243 = f("bhdma243");
                let bhdmb561 = f("bhdmb561");
                let bhdmb562 = f("bhdmb562");
                let bhdmb987 = f("bhdmb987");
                let bhdmb993 = f("bhdmb993");
                let bhdmf560 = f("bhdmf560");
                let bhdmf576 = f("bhdmf576");
                let bhdmf577 = f("bhdmf577");
                let bhdmf578 = f("bhdmf578");
                let bhdmf579 = f("bhdmf579");
                let bhdmf580 = f("bhdmf580");
                let bhdmf581 = f("bhdmf581");
                let bhdmf582 = f("bhdmf582");
                let bhdmf583 = f("bhdmf583");
                let bhdmf584 = f("bhdmf584");
                let bhdmf585 = f("bhdmf585");
                let bhdmf586 = f("bhdmf586");
                let bhdmf587 = f("bhdmf587");
                let bhdmf588 = f("bhdmf588");
                let bhdmf589 = f("bhdmf589");
                let bhdmf590 = f("bhdmf590");
                let bhdmf591 = f("bhdmf591");
                let bhdmf592 = f("bhdmf592");
                let bhdmf593 = f("bhdmf593");
                let bhdmf594 = f("bhdmf594");
                let bhdmf595 = f("bhdmf595");
                let bhdmf596 = f("bhdmf596");
                let bhdmf597 = f("bhdmf597");
                let bhdmf598 = f("bhdmf598");
                let bhdmf599 = f("bhdmf599");
                let bhdmf600 = f("bhdmf600");
                let bhdmf601 = f("bhdmf601");
                let bhdmf604 = f("bhdmf604");
                let bhdmf605 = f("bhdmf605");
                let bhdmf606 = f("bhdmf606");
                let bhdmf607 = f("bhdmf607");
                let bhdmf611 = f("bhdmf611");
                let bhdmf612 = f("bhdmf612");
                let bhdmf613 = f("bhdmf613");
                let bhdmf614 = f("bhdmf614");
                let bhdmf615 = f("bhdmf615");
                let bhdmf616 = f("bhdmf616");
                let bhdmf617 = f("bhdmf617");
                let bhdmf618 = f("bhdmf618");
                let bhdmf624 = f("bhdmf624");
                let bhdmf625 = f("bhdmf625");
                let bhdmf626 = f("bhdmf626");
                let bhdmf627 = f("bhdmf627");
                let bhdmf628 = f("bhdmf628");
                let bhdmf629 = f("bhdmf629");
                let bhdmf630 = f("bhdmf630");
                let bhdmf631 = f("bhdmf631");
                let bhdmf632 = f("bhdmf632");
                let bhdmf633 = f("bhdmf633");
                let bhdmf634 = f("bhdmf634");
                let bhdmf635 = f("bhdmf635");
                let bhdmf636 = f("bhdmf636");
                let bhdmf639 = f("bhdmf639");
                let bhdmf640 = f("bhdmf640");
                let bhdmf670 = f("bhdmf670");
                let bhdmf671 = f("bhdmf671");
                let bhdmf672 = f("bhdmf672");
                let bhdmf673 = f("bhdmf673");
                let bhdmf674 = f("bhdmf674");
                let bhdmf675 = f("bhdmf675");
                let bhdmf676 = f("bhdmf676");
                let bhdmf677 = f("bhdmf677");
                let bhdmf678 = f("bhdmf678");
                let bhdmf679 = f("bhdmf679");
                let bhdmf680 = f("bhdmf680");
                let bhdmf681 = f("bhdmf681");
                let bhdmf724 = f("bhdmf724");
                let bhdmg209 = f("bhdmg209");
                let bhdmg210 = f("bhdmg210");
                let bhdmg211 = f("bhdmg211");
                let bhdmg299 = f("bhdmg299");
                let bhdmg332 = f("bhdmg332");
                let bhdmg333 = f("bhdmg333");
                let bhdmg334 = f("bhdmg334");
                let bhdmg335 = f("bhdmg335");
                let bhdmg379 = f("bhdmg379");
                let bhdmg380 = f("bhdmg380");
                let bhdmg381 = f("bhdmg381");
                let bhdmg382 = f("bhdmg382");
                let bhdmg383 = f("bhdmg383");
                let bhdmg384 = f("bhdmg384");
                let bhdmg385 = f("bhdmg385");
                let bhdmg386 = f("bhdmg386");
                let bhdmg387 = f("bhdmg387");
                let bhdmg388 = f("bhdmg388");
                let bhdmg651 = f("bhdmg651");
                let bhdmg652 = f("bhdmg652");
                let bhdmhk06 = f("bhdmhk06");
                let bhdmhk31 = f("bhdmhk31");
                let bhdmhk32 = f("bhdmhk32");
                let bhdmj451 = f("bhdmj451");
                let bhdmj454 = f("bhdmj454");
                let bhdmk045 = f("bhdmk045");
                let bhdmk046 = f("bhdmk046");
                let bhdmk047 = f("bhdmk047");
                let bhdmk048 = f("bhdmk048");
                let bhdmk049 = f("bhdmk049");
                let bhdmk050 = f("bhdmk050");
                let bhdmk051 = f("bhdmk051");
                let bhdmk052 = f("bhdmk052");
                let bhdmk053 = f("bhdmk053");
                let bhdmk054 = f("bhdmk054");
                let bhdmk055 = f("bhdmk055");
                let bhdmk056 = f("bhdmk056");
                let bhdmk057 = f("bhdmk057");
                let bhdmk058 = f("bhdmk058");
                let bhdmk059 = f("bhdmk059");
                let bhdmk060 = f("bhdmk060");
                let bhdmk061 = f("bhdmk061");
                let bhdmk062 = f("bhdmk062");
                let bhdmk063 = f("bhdmk063");
                let bhdmk064 = f("bhdmk064");
                let bhdmk065 = f("bhdmk065");
                let bhdmk066 = f("bhdmk066");
                let bhdmk067 = f("bhdmk067");
                let bhdmk068 = f("bhdmk068");
                let bhdmk069 = f("bhdmk069");
                let bhdmk070 = f("bhdmk070");
                let bhdmk071 = f("bhdmk071");
                let bhdmk105 = f("bhdmk105");
                let bhdmk106 = f("bhdmk106");
                let bhdmk107 = f("bhdmk107");
                let bhdmk108 = f("bhdmk108");
                let bhdmk109 = f("bhdmk109");
                let bhdmk110 = f("bhdmk110");
                let bhdmk111 = f("bhdmk111");
                let bhdmk112 = f("bhdmk112");
                let bhdmk113 = f("bhdmk113");
                let bhdmk114 = f("bhdmk114");
                let bhdmk115 = f("bhdmk115");
                let bhdmk116 = f("bhdmk116");
                let bhdmk117 = f("bhdmk117");
                let bhdmk118 = f("bhdmk118");
                let bhdmk119 = f("bhdmk119");
                let bhdmk130 = f("bhdmk130");
                let bhdmk131 = f("bhdmk131");
                let bhdmk132 = f("bhdmk132");
                let bhdmk158 = f("bhdmk158");
                let bhdmk159 = f("bhdmk159");
                let bhdmk160 = f("bhdmk160");
                let bhdmk161 = f("bhdmk161");
                let bhdmk162 = f("bhdmk162");
                let bhdmk166 = f("bhdmk166");
                let bhdmk169 = f("bhdmk169");
                let bhdmk170 = f("bhdmk170");
                let bhdmk171 = f("bhdmk171");
                let bhdmk172 = f("bhdmk172");
                let bhdmk173 = f("bhdmk173");
                let bhdmk174 = f("bhdmk174");
                let bhdmk175 = f("bhdmk175");
                let bhdmk176 = f("bhdmk176");
                let bhdmk177 = f("bhdmk177");
                let bhdmk187 = f("bhdmk187");
                let bhdmk188 = f("bhdmk188");
                let bhdmk189 = f("bhdmk189");
                let bhdmk190 = f("bhdmk190");
                let bhdmk191 = f("bhdmk191");
                let bhdmk195 = f("bhdmk195");
                let bhdmk196 = f("bhdmk196");
                let bhdmk197 = f("bhdmk197");
                let bhdmk198 = f("bhdmk198");
                let bhdmk199 = f("bhdmk199");
                let bhdmk200 = f("bhdmk200");
                let bhdmk208 = f("bhdmk208");
                let bhdmk209 = f("bhdmk209");
                let bhdmk210 = f("bhdmk210");
                let bhdmk211 = f("bhdmk211");
                let bhdmkx57 = f("bhdmkx57");
                let bhfn3360 = f("bhfn3360");
                let bhfn3543 = f("bhfn3543");
                let bhfn6631 = f("bhfn6631");
                let bhfn6636 = f("bhfn6636");
                let bhfna245 = f("bhfna245");
                let bhfnk260 = f("bhfnk260");
                let bhod2389 = f("bhod2389");
                let bhod2604 = f("bhod2604");
                let bhod3187 = f("bhod3187");
                let bhod3189 = f("bhod3189");
                let bhod6648 = f("bhod6648");
                let bhodhk29 = f("bhodhk29");
                let bhodj474 = f("bhodj474");
                let bhpa0365 = f("bhpa0365");
                let bhpa4340 = f("bhpa4340");
                let bhpx8901 = s("bhpx8901");
                let bhsp0010 = f("bhsp0010");
                let bhsp0027 = f("bhsp0027");
                let bhsp0087 = f("bhsp0087");
                let bhsp0088 = f("bhsp0088");
                let bhsp0089 = f("bhsp0089");
                let bhsp0201 = f("bhsp0201");
                let bhsp0202 = f("bhsp0202");
                let bhsp0206 = f("bhsp0206");
                let bhsp0390 = f("bhsp0390");
                let bhsp0416 = f("bhsp0416");
                let bhsp0447 = f("bhsp0447");
                let bhsp0496 = f("bhsp0496");
                let bhsp0508 = f("bhsp0508");
                let bhsp0523 = f("bhsp0523");
                let bhsp0530 = f("bhsp0530");
                let bhsp1283 = f("bhsp1283");
                let bhsp2111 = f("bhsp2111");
                let bhsp2112 = f("bhsp2112");
                let bhsp2122 = f("bhsp2122");
                let bhsp2145 = f("bhsp2145");
                let bhsp2148 = f("bhsp2148");
                let bhsp2170 = f("bhsp2170");
                let bhsp2309 = f("bhsp2309");
                let bhsp2723 = f("bhsp2723");
                let bhsp2724 = f("bhsp2724");
                let bhsp2792 = f("bhsp2792");
                let bhsp2794 = f("bhsp2794");
                let bhsp2796 = f("bhsp2796");
                let bhsp2932 = f("bhsp2932");
                let bhsp3049 = f("bhsp3049");
                let bhsp3066 = f("bhsp3066");
                let bhsp3123 = f("bhsp3123");
                let bhsp3148 = f("bhsp3148");
                let bhsp3151 = f("bhsp3151");
                let bhsp3152 = f("bhsp3152");
                let bhsp3153 = f("bhsp3153");
                let bhsp3154 = f("bhsp3154");
                let bhsp3155 = f("bhsp3155");
                let bhsp3156 = f("bhsp3156");
                let bhsp3158 = f("bhsp3158");
                let bhsp3166 = f("bhsp3166");
                let bhsp3167 = f("bhsp3167");
                let bhsp3210 = f("bhsp3210");
                let bhsp3230 = f("bhsp3230");
                let bhsp3238 = f("bhsp3238");
                let bhsp3239 = f("bhsp3239");
                let bhsp3247 = f("bhsp3247");
                let bhsp3283 = f("bhsp3283");
                let bhsp3300 = f("bhsp3300");
                let bhsp3513 = f("bhsp3513");
                let bhsp3523 = f("bhsp3523");
                let bhsp3524 = f("bhsp3524");
                let bhsp3525 = f("bhsp3525");
                let bhsp3526 = f("bhsp3526");
                let bhsp3527 = f("bhsp3527");
                let bhsp3605 = f("bhsp3605");
                let bhsp3620 = f("bhsp3620");
                let bhsp3621 = f("bhsp3621");
                let bhsp4000 = f("bhsp4000");
                let bhsp4073 = f("bhsp4073");
                let bhsp4093 = f("bhsp4093");
                let bhsp4130 = f("bhsp4130");
                let bhsp4250 = f("bhsp4250");
                let bhsp4302 = f("bhsp4302");
                let bhsp4336 = f("bhsp4336");
                let bhsp4340 = f("bhsp4340");
                let bhsp4778 = f("bhsp4778");
                let bhsp5993 = f("bhsp5993");
                let bhsp6416 = f("bhsp6416");
                let bhsp6649 = f("bhsp6649");
                let bhsp6796 = f("bhsp6796");
                let bhsp6797 = f("bhsp6797");
                let bhsp8434 = f("bhsp8434");
                let bhsp8516 = f("bhsp8516");
                let bhsp8517 = f("bhsp8517");
                let bhsp8519 = f("bhsp8519");
                let bhsp8520 = f("bhsp8520");
                let bhsp8521 = f("bhsp8521");
                let bhsp8522 = f("bhsp8522");
                let bhsp8523 = f("bhsp8523");
                let bhsp8524 = f("bhsp8524");
                let bhsp8525 = f("bhsp8525");
                let bhsp8526 = f("bhsp8526");
                let bhsp8527 = f("bhsp8527");
                let bhsp8528 = f("bhsp8528");
                let bhsp8529 = f("bhsp8529");
                let bhsp8530 = f("bhsp8530");
                let bhsp8843 = f("bhsp8843");
                let bhsp9191 = f("bhsp9191");
                let bhsp9802 = f("bhsp9802");
                let bhspa024 = f("bhspa024");
                let bhspa130 = f("bhspa130");
                let bhspa530 = f("bhspa530");
                let bhspb530 = f("bhspb530");
                let bhspc009 = f("bhspc009");
                let bhspc159 = f("bhspc159");
                let bhspc160 = b("bhspc160");
                let bhspc161 = f("bhspc161");
                let bhspc252 = f("bhspc252");
                let bhspc253 = f("bhspc253");
                let bhspc254 = f("bhspc254");
                let bhspc255 = f("bhspc255");
                let bhspc256 = f("bhspc256");
                let bhspc257 = f("bhspc257");
                let bhspc427 = f("bhspc427");
                let bhspc428 = f("bhspc428");
                let bhspc447 = f("bhspc447");
                let bhspc700 = f("bhspc700");
                let bhspc701 = f("bhspc701");
                let bhspc702 = f("bhspc702");
                let bhspc884 = f("bhspc884");
                let bhspf074 = f("bhspf074");
                let bhspf075 = f("bhspf075");
                let bhspf229 = f("bhspf229");
                let bhspf819 = f("bhspf819");
                let bhspf820 = f("bhspf820");
                let bhspf838 = f("bhspf838");
                let bhspf841 = b("bhspf841");
                let bhspf842 = b("bhspf842");
                let bhspft28 = f("bhspft28");
                let bhspft42 = b("bhspft42");
                let bhspft43 = b("bhspft43");
                let bhspft44 = b("bhspft44");
                let bhspg234 = f("bhspg234");
                let bhspg235 = f("bhspg235");
                let bhspht69 = f("bhspht69");
                let bhspht70 = f("bhspht70");
                let bhspht95 = f("bhspht95");
                let bhspj980 = f("bhspj980");
                let bhspk141 = f("bhspk141");
                let bhspky38 = b("bhspky38");
                let bhspm962 = f("bhspm962");
                let bhspmz36 = f("bhspmz36");
                let bhspnk60 = b("bhspnk60");
                let bhsx8901 = s("bhsx8901");
                let bhtxf655 = s("bhtxf655");
                let bhtxf656 = s("bhtxf656");
                let bhtxf657 = s("bhtxf657");
                let bhtxf658 = s("bhtxf658");
                let bhtxf659 = s("bhtxf659");
                let bhtxf660 = s("bhtxf660");
                let bhtxg546 = s("bhtxg546");
                let bhtxg551 = s("bhtxg551");
                let bhtxg556 = s("bhtxg556");
                let bhtxg561 = s("bhtxg561");
                let bhtxg571 = s("bhtxg571");
                let bhtxg576 = b("bhtxg576");
                let bhtxg581 = s("bhtxg581");
                let bhtxg586 = s("bhtxg586");
                let rssd4087 = s("rssd4087");
                let rssd6191 = f("rssd6191");
                let rssd9001 = f("rssd9001");
                let rssd9005 = s("rssd9005");
                let rssd9007 = d("rssd9007");
                let rssd9008 = d("rssd9008");
                let rssd9010 = s("rssd9010");
                let rssd9014 = f("rssd9014");
                let rssd9016 = f("rssd9016");
                let rssd9017 = s("rssd9017");
                let rssd9028 = s("rssd9028");
                let rssd9029 = s("rssd9029");
                let rssd9030 = f("rssd9030");
                let rssd9031 = f("rssd9031");
                let rssd9032 = f("rssd9032");
                let rssd9037 = f("rssd9037");
                let rssd9038 = s("rssd9038");
                let rssd9039 = f("rssd9039");
                let rssd9042 = f("rssd9042");
                let rssd9044 = f("rssd9044");
                let rssd9045 = f("rssd9045");
                let rssd9046 = f("rssd9046");
                let rssd9047 = f("rssd9047");
                let rssd9048 = f("rssd9048");
                let rssd9049 = f("rssd9049");
                let rssd9050 = f("rssd9050");
                let rssd9052 = f("rssd9052");
                let rssd9053 = f("rssd9053");
                let rssd9054 = f("rssd9054");
                let rssd9055 = f("rssd9055");
                let rssd9056 = f("rssd9056");
                let rssd9059 = f("rssd9059");
                let rssd9060 = f("rssd9060");
                let rssd9061 = f("rssd9061");
                let rssd9101 = s("rssd9101");
                let rssd9130 = s("rssd9130");
                let rssd9132 = f("rssd9132");
                let rssd9138 = f("rssd9138");
                let rssd9146 = f("rssd9146");
                let rssd9150 = f("rssd9150");
                let rssd9161 = s("rssd9161");
                let rssd9170 = f("rssd9170");
                let rssd9192 = s("rssd9192");
                let rssd9198 = f("rssd9198");
                let rssd9200 = s("rssd9200");
                let rssd9210 = f("rssd9210");
                let rssd9213 = f("rssd9213");
                let rssd9216 = f("rssd9216");
                let rssd9220 = s("rssd9220");
                let rssd9320 = f("rssd9320");
                let rssd9374 = f("rssd9374");
                let rssd9375 = f("rssd9375");
                let rssd9421 = f("rssd9421");
                let rssd9422 = f("rssd9422");
                let rssd9424 = f("rssd9424");
                let rssd9425 = f("rssd9425");
                let rssd9579 = f("rssd9579");
                let rssd9950 = d("rssd9950");
                let rssd9955 = f("rssd9955");
                let rssd9999 = f("rssd9999");
                let texc3573 = b("texc3573");
                let texc3575 = b("texc3575");
                let texc6373 = f("texc6373");
                let texc6561 = b("texc6561");
                let texc6562 = b("texc6562");
                let texc6568 = b("texc6568");
                let texc6586 = b("texc6586");
                let texc6995 = b("texc6995");
                let texc6996 = b("texc6996");
                let texc6997 = b("texc6997");
                let texc6998 = b("texc6998");
                let texc8520 = f("texc8520");
                let texc8521 = b("texc8521");
                let texc8522 = b("texc8522");
                let texc8523 = f("texc8523");
                let texc8524 = f("texc8524");
                let texc8525 = b("texc8525");
                let texc8557 = f("texc8557");
                let texc8558 = f("texc8558");
                let texc8559 = f("texc8559");
                let texc8562 = f("texc8562");
                let texc8563 = f("texc8563");
                let texc8564 = f("texc8564");
                let texc8565 = f("texc8565");
                let texc8566 = f("texc8566");
                let texc8567 = f("texc8567");
                let text3571 = s("text3571");
                let text3573 = s("text3573");
                let text3575 = s("text3575");
                let text4769 = b("text4769");
                let text5351 = s("text5351");
                let text5352 = s("text5352");
                let text5353 = s("text5353");
                let text5354 = s("text5354");
                let text5355 = s("text5355");
                let text5356 = s("text5356");
                let text5357 = s("text5357");
                let text5358 = s("text5358");
                let text5359 = s("text5359");
                let text5360 = s("text5360");
                let text5485 = s("text5485");
                let text5486 = s("text5486");
                let text5487 = s("text5487");
                let text5488 = s("text5488");
                let text5489 = s("text5489");
                let text5523 = b("text5523");
                let text6373 = s("text6373");
                let text6561 = s("text6561");
                let text6562 = s("text6562");
                let text6568 = s("text6568");
                let text6586 = s("text6586");
                let text6995 = b("text6995");
                let text6996 = b("text6996");
                let text6997 = b("text6997");
                let text6998 = b("text6998");
                let text8520 = s("text8520");
                let text8521 = s("text8521");
                let text8522 = s("text8522");
                let text8523 = s("text8523");
                let text8524 = s("text8524");
                let text8525 = s("text8525");
                let text8526 = s("text8526");
                let text8527 = s("text8527");
                let text8528 = s("text8528");
                let text8529 = s("text8529");
                let text8530 = s("text8530");
                let text8557 = s("text8557");
                let text8558 = s("text8558");
                let text8559 = s("text8559");
                let text8562 = s("text8562");
                let text8563 = s("text8563");
                let text8564 = s("text8564");
                let text8565 = s("text8565");
                let text8566 = s("text8566");
                let text8567 = s("text8567");
                let textb027 = s("textb027");
                let textb028 = s("textb028");
                let textb029 = s("textb029");
                let textb030 = s("textb030");
                let textb031 = s("textb031");
                let textb032 = s("textb032");
                let textb033 = s("textb033");
                let textb034 = s("textb034");
                let textb035 = s("textb035");
                let textb036 = s("textb036");
                let textb037 = s("textb037");
                let textb038 = s("textb038");
                let textb039 = s("textb039");
                let textb040 = s("textb040");
                let textb041 = s("textb041");
                let textb042 = s("textb042");
                let textb043 = s("textb043");
                let textb044 = s("textb044");
                let textb045 = s("textb045");
                let textb046 = s("textb046");
                let textb047 = s("textb047");
                let textb048 = s("textb048");
                let textb049 = s("textb049");
                let textb050 = s("textb050");
                let textb051 = s("textb051");
                let textb052 = s("textb052");
                let textb053 = s("textb053");
                let textb054 = b("textb054");
                let textb055 = b("textb055");
                let textb056 = s("textb056");
                let textc231 = s("textc231");
                let textc490 = b("textc490");
                let textc497 = s("textc497");
                let textc703 = s("textc703");
                let textc708 = s("textc708");
                let textc714 = s("textc714");
                let textc715 = s("textc715");
                let textft29 = b("textft29");
                let textft31 = s("textft31");
                let wrdsdownloaddate = d("wrdsdownloaddate");

                for row_i in 0..batch.num_rows() {
                    let mut vals: Vec<AnyValue<'static>> = Vec::with_capacity(1086);
                    vals.push(if bhbc3368.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc3368.value(row_i))
                    });
                    vals.push(if bhbc3402.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc3402.value(row_i))
                    });
                    vals.push(if bhbc3516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc3516.value(row_i))
                    });
                    vals.push(if bhbc3519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc3519.value(row_i))
                    });
                    vals.push(if bhbc4070.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4070.value(row_i))
                    });
                    vals.push(if bhbc4073.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4073.value(row_i))
                    });
                    vals.push(if bhbc4074.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4074.value(row_i))
                    });
                    vals.push(if bhbc4079.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4079.value(row_i))
                    });
                    vals.push(if bhbc4091.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4091.value(row_i))
                    });
                    vals.push(if bhbc4093.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4093.value(row_i))
                    });
                    vals.push(if bhbc4094.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4094.value(row_i))
                    });
                    vals.push(if bhbc4107.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4107.value(row_i))
                    });
                    vals.push(if bhbc4135.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4135.value(row_i))
                    });
                    vals.push(if bhbc4218.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4218.value(row_i))
                    });
                    vals.push(if bhbc4230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4230.value(row_i))
                    });
                    vals.push(if bhbc4301.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4301.value(row_i))
                    });
                    vals.push(if bhbc4302.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4302.value(row_i))
                    });
                    vals.push(if bhbc4320.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4320.value(row_i))
                    });
                    vals.push(if bhbc4340.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4340.value(row_i))
                    });
                    vals.push(if bhbc4421.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4421.value(row_i))
                    });
                    vals.push(if bhbc4475.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4475.value(row_i))
                    });
                    vals.push(if bhbc4484.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4484.value(row_i))
                    });
                    vals.push(if bhbc4519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc4519.value(row_i))
                    });
                    vals.push(if bhbc6061.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbc6061.value(row_i))
                    });
                    vals.push(if bhbca220.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbca220.value(row_i))
                    });
                    vals.push(if bhbcb490.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbcb490.value(row_i))
                    });
                    vals.push(if bhbcb491.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbcb491.value(row_i))
                    });
                    vals.push(if bhbcb493.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbcb493.value(row_i))
                    });
                    vals.push(if bhbcb494.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbcb494.value(row_i))
                    });
                    vals.push(if bhbcc216.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbcc216.value(row_i))
                    });
                    vals.push(if bhbcjj33.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhbcjj33.value(row_i))
                    });
                    vals.push(if bhc00010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc00010.value(row_i))
                    });
                    vals.push(if bhc00390.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc00390.value(row_i))
                    });
                    vals.push(if bhc01350.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc01350.value(row_i))
                    });
                    vals.push(if bhc01754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc01754.value(row_i))
                    });
                    vals.push(if bhc01773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc01773.value(row_i))
                    });
                    vals.push(if bhc02122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc02122.value(row_i))
                    });
                    vals.push(if bhc02170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc02170.value(row_i))
                    });
                    vals.push(if bhc03411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc03411.value(row_i))
                    });
                    vals.push(if bhc03429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc03429.value(row_i))
                    });
                    vals.push(if bhc03433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc03433.value(row_i))
                    });
                    vals.push(if bhc03545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc03545.value(row_i))
                    });
                    vals.push(if bhc05369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc05369.value(row_i))
                    });
                    vals.push(if bhc06551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06551.value(row_i))
                    });
                    vals.push(if bhc06563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06563.value(row_i))
                    });
                    vals.push(if bhc06566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06566.value(row_i))
                    });
                    vals.push(if bhc06570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06570.value(row_i))
                    });
                    vals.push(if bhc06572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06572.value(row_i))
                    });
                    vals.push(if bhc06574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06574.value(row_i))
                    });
                    vals.push(if bhc06575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06575.value(row_i))
                    });
                    vals.push(if bhc06598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06598.value(row_i))
                    });
                    vals.push(if bhc06601.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhc06601.value(row_i))
                    });
                    vals.push(if bhc06602.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06602.value(row_i))
                    });
                    vals.push(if bhc06603.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc06603.value(row_i))
                    });
                    vals.push(if bhc0a167.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0a167.value(row_i))
                    });
                    vals.push(if bhc0a250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0a250.value(row_i))
                    });
                    vals.push(if bhc0b528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0b528.value(row_i))
                    });
                    vals.push(if bhc0b546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0b546.value(row_i))
                    });
                    vals.push(if bhc0b639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0b639.value(row_i))
                    });
                    vals.push(if bhc0b675.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0b675.value(row_i))
                    });
                    vals.push(if bhc0b681.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0b681.value(row_i))
                    });
                    vals.push(if bhc0c225.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0c225.value(row_i))
                    });
                    vals.push(if bhc0g591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc0g591.value(row_i))
                    });
                    vals.push(if bhc20010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc20010.value(row_i))
                    });
                    vals.push(if bhc20390.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc20390.value(row_i))
                    });
                    vals.push(if bhc21350.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc21350.value(row_i))
                    });
                    vals.push(if bhc21754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc21754.value(row_i))
                    });
                    vals.push(if bhc21773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc21773.value(row_i))
                    });
                    vals.push(if bhc22122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc22122.value(row_i))
                    });
                    vals.push(if bhc22170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc22170.value(row_i))
                    });
                    vals.push(if bhc23411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc23411.value(row_i))
                    });
                    vals.push(if bhc23429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc23429.value(row_i))
                    });
                    vals.push(if bhc23433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc23433.value(row_i))
                    });
                    vals.push(if bhc23545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc23545.value(row_i))
                    });
                    vals.push(if bhc25369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc25369.value(row_i))
                    });
                    vals.push(if bhc26551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26551.value(row_i))
                    });
                    vals.push(if bhc26563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26563.value(row_i))
                    });
                    vals.push(if bhc26566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26566.value(row_i))
                    });
                    vals.push(if bhc26570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26570.value(row_i))
                    });
                    vals.push(if bhc26572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26572.value(row_i))
                    });
                    vals.push(if bhc26574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26574.value(row_i))
                    });
                    vals.push(if bhc26575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26575.value(row_i))
                    });
                    vals.push(if bhc26598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26598.value(row_i))
                    });
                    vals.push(if bhc26601.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26601.value(row_i))
                    });
                    vals.push(if bhc26602.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26602.value(row_i))
                    });
                    vals.push(if bhc26603.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc26603.value(row_i))
                    });
                    vals.push(if bhc2a167.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2a167.value(row_i))
                    });
                    vals.push(if bhc2a250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2a250.value(row_i))
                    });
                    vals.push(if bhc2b528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2b528.value(row_i))
                    });
                    vals.push(if bhc2b546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2b546.value(row_i))
                    });
                    vals.push(if bhc2b639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2b639.value(row_i))
                    });
                    vals.push(if bhc2b675.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2b675.value(row_i))
                    });
                    vals.push(if bhc2b681.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2b681.value(row_i))
                    });
                    vals.push(if bhc2c225.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2c225.value(row_i))
                    });
                    vals.push(if bhc2g591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc2g591.value(row_i))
                    });
                    vals.push(if bhc50390.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc50390.value(row_i))
                    });
                    vals.push(if bhc51350.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhc51350.value(row_i))
                    });
                    vals.push(if bhc51754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc51754.value(row_i))
                    });
                    vals.push(if bhc51773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc51773.value(row_i))
                    });
                    vals.push(if bhc52122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc52122.value(row_i))
                    });
                    vals.push(if bhc52170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc52170.value(row_i))
                    });
                    vals.push(if bhc53411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc53411.value(row_i))
                    });
                    vals.push(if bhc53433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc53433.value(row_i))
                    });
                    vals.push(if bhc53545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc53545.value(row_i))
                    });
                    vals.push(if bhc55369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc55369.value(row_i))
                    });
                    vals.push(if bhc56551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56551.value(row_i))
                    });
                    vals.push(if bhc56563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56563.value(row_i))
                    });
                    vals.push(if bhc56566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56566.value(row_i))
                    });
                    vals.push(if bhc56570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56570.value(row_i))
                    });
                    vals.push(if bhc56572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56572.value(row_i))
                    });
                    vals.push(if bhc56574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56574.value(row_i))
                    });
                    vals.push(if bhc56575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56575.value(row_i))
                    });
                    vals.push(if bhc56598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56598.value(row_i))
                    });
                    vals.push(if bhc56602.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56602.value(row_i))
                    });
                    vals.push(if bhc56603.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc56603.value(row_i))
                    });
                    vals.push(if bhc5a167.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5a167.value(row_i))
                    });
                    vals.push(if bhc5a250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5a250.value(row_i))
                    });
                    vals.push(if bhc5b528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5b528.value(row_i))
                    });
                    vals.push(if bhc5b546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5b546.value(row_i))
                    });
                    vals.push(if bhc5b639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5b639.value(row_i))
                    });
                    vals.push(if bhc5b675.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5b675.value(row_i))
                    });
                    vals.push(if bhc5b681.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5b681.value(row_i))
                    });
                    vals.push(if bhc5g591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc5g591.value(row_i))
                    });
                    vals.push(if bhc90010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc90010.value(row_i))
                    });
                    vals.push(if bhc90390.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc90390.value(row_i))
                    });
                    vals.push(if bhc91350.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc91350.value(row_i))
                    });
                    vals.push(if bhc91727.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc91727.value(row_i))
                    });
                    vals.push(if bhc91754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc91754.value(row_i))
                    });
                    vals.push(if bhc91773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc91773.value(row_i))
                    });
                    vals.push(if bhc92122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc92122.value(row_i))
                    });
                    vals.push(if bhc92170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc92170.value(row_i))
                    });
                    vals.push(if bhc93411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc93411.value(row_i))
                    });
                    vals.push(if bhc93429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc93429.value(row_i))
                    });
                    vals.push(if bhc93433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc93433.value(row_i))
                    });
                    vals.push(if bhc93545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc93545.value(row_i))
                    });
                    vals.push(if bhc95369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc95369.value(row_i))
                    });
                    vals.push(if bhc96551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96551.value(row_i))
                    });
                    vals.push(if bhc96563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96563.value(row_i))
                    });
                    vals.push(if bhc96566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96566.value(row_i))
                    });
                    vals.push(if bhc96570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96570.value(row_i))
                    });
                    vals.push(if bhc96572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96572.value(row_i))
                    });
                    vals.push(if bhc96574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96574.value(row_i))
                    });
                    vals.push(if bhc96575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96575.value(row_i))
                    });
                    vals.push(if bhc96598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96598.value(row_i))
                    });
                    vals.push(if bhc96602.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96602.value(row_i))
                    });
                    vals.push(if bhc96603.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc96603.value(row_i))
                    });
                    vals.push(if bhc9a250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9a250.value(row_i))
                    });
                    vals.push(if bhc9b528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9b528.value(row_i))
                    });
                    vals.push(if bhc9b541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9b541.value(row_i))
                    });
                    vals.push(if bhc9b546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9b546.value(row_i))
                    });
                    vals.push(if bhc9b639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9b639.value(row_i))
                    });
                    vals.push(if bhc9b675.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9b675.value(row_i))
                    });
                    vals.push(if bhc9b681.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9b681.value(row_i))
                    });
                    vals.push(if bhc9c225.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9c225.value(row_i))
                    });
                    vals.push(if bhc9g591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhc9g591.value(row_i))
                    });
                    vals.push(if bhca2170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca2170.value(row_i))
                    });
                    vals.push(if bhca3792.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca3792.value(row_i))
                    });
                    vals.push(if bhca5310.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca5310.value(row_i))
                    });
                    vals.push(if bhca5311.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca5311.value(row_i))
                    });
                    vals.push(if bhca7204.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca7204.value(row_i))
                    });
                    vals.push(if bhca7205.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca7205.value(row_i))
                    });
                    vals.push(if bhca7206.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca7206.value(row_i))
                    });
                    vals.push(if bhca8274.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhca8274.value(row_i))
                    });
                    vals.push(if bhcaa223.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcaa223.value(row_i))
                    });
                    vals.push(if bhcaa224.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcaa224.value(row_i))
                    });
                    vals.push(if bhcab530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcab530.value(row_i))
                    });
                    vals.push(if bhcab596.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcab596.value(row_i))
                    });
                    vals.push(if bhcah036.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcah036.value(row_i))
                    });
                    vals.push(if bhcah311.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcah311.value(row_i))
                    });
                    vals.push(if bhcah312.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcah312.value(row_i))
                    });
                    vals.push(if bhcah313.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcah313.value(row_i))
                    });
                    vals.push(if bhcah314.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcah314.value(row_i))
                    });
                    vals.push(if bhcajj29.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcajj29.value(row_i))
                    });
                    vals.push(if bhcakw00.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakw00.value(row_i))
                    });
                    vals.push(if bhcakw03.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakw03.value(row_i))
                    });
                    vals.push(if bhcakx77.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakx77.value(row_i))
                    });
                    vals.push(if bhcakx78.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakx78.value(row_i))
                    });
                    vals.push(if bhcakx79.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakx79.value(row_i))
                    });
                    vals.push(if bhcakx80.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakx80.value(row_i))
                    });
                    vals.push(if bhcakx81.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakx81.value(row_i))
                    });
                    vals.push(if bhcakx82.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakx82.value(row_i))
                    });
                    vals.push(if bhcakx83.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcakx83.value(row_i))
                    });
                    vals.push(if bhcalb58.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalb58.value(row_i))
                    });
                    vals.push(if bhcalb59.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalb59.value(row_i))
                    });
                    vals.push(if bhcalb60.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalb60.value(row_i))
                    });
                    vals.push(if bhcalb61.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalb61.value(row_i))
                    });
                    vals.push(if bhcale74.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale74.value(row_i))
                    });
                    vals.push(if bhcale85.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale85.value(row_i))
                    });
                    vals.push(if bhcale86.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale86.value(row_i))
                    });
                    vals.push(if bhcale87.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale87.value(row_i))
                    });
                    vals.push(if bhcale88.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale88.value(row_i))
                    });
                    vals.push(if bhcale89.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale89.value(row_i))
                    });
                    vals.push(if bhcale90.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale90.value(row_i))
                    });
                    vals.push(if bhcale91.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale91.value(row_i))
                    });
                    vals.push(if bhcale92.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcale92.value(row_i))
                    });
                    vals.push(if bhcalf21.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalf21.value(row_i))
                    });
                    vals.push(if bhcalf22.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalf22.value(row_i))
                    });
                    vals.push(if bhcalf23.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalf23.value(row_i))
                    });
                    vals.push(if bhcalf24.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalf24.value(row_i))
                    });
                    vals.push(if bhcalf25.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalf25.value(row_i))
                    });
                    vals.push(if bhcalf27.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalf27.value(row_i))
                    });
                    vals.push(if bhcalf28.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcalf28.value(row_i))
                    });
                    vals.push(if bhcamk66.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcamk66.value(row_i))
                    });
                    vals.push(if bhcamk76.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcamk76.value(row_i))
                    });
                    vals.push(if bhcamk77.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcamk77.value(row_i))
                    });
                    vals.push(if bhcamk78.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcamk78.value(row_i))
                    });
                    vals.push(if bhcanc99.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcanc99.value(row_i))
                    });
                    vals.push(if bhcap742.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap742.value(row_i))
                    });
                    vals.push(if bhcap793.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap793.value(row_i))
                    });
                    vals.push(if bhcap838.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap838.value(row_i))
                    });
                    vals.push(if bhcap839.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap839.value(row_i))
                    });
                    vals.push(if bhcap840.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap840.value(row_i))
                    });
                    vals.push(if bhcap841.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap841.value(row_i))
                    });
                    vals.push(if bhcap842.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap842.value(row_i))
                    });
                    vals.push(if bhcap843.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap843.value(row_i))
                    });
                    vals.push(if bhcap844.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap844.value(row_i))
                    });
                    vals.push(if bhcap845.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap845.value(row_i))
                    });
                    vals.push(if bhcap846.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap846.value(row_i))
                    });
                    vals.push(if bhcap847.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap847.value(row_i))
                    });
                    vals.push(if bhcap848.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap848.value(row_i))
                    });
                    vals.push(if bhcap849.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap849.value(row_i))
                    });
                    vals.push(if bhcap850.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap850.value(row_i))
                    });
                    vals.push(if bhcap851.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap851.value(row_i))
                    });
                    vals.push(if bhcap852.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap852.value(row_i))
                    });
                    vals.push(if bhcap853.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap853.value(row_i))
                    });
                    vals.push(if bhcap854.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap854.value(row_i))
                    });
                    vals.push(if bhcap855.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap855.value(row_i))
                    });
                    vals.push(if bhcap856.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap856.value(row_i))
                    });
                    vals.push(if bhcap857.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap857.value(row_i))
                    });
                    vals.push(if bhcap858.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap858.value(row_i))
                    });
                    vals.push(if bhcap859.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap859.value(row_i))
                    });
                    vals.push(if bhcap860.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap860.value(row_i))
                    });
                    vals.push(if bhcap861.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap861.value(row_i))
                    });
                    vals.push(if bhcap862.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap862.value(row_i))
                    });
                    vals.push(if bhcap863.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap863.value(row_i))
                    });
                    vals.push(if bhcap864.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap864.value(row_i))
                    });
                    vals.push(if bhcap865.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap865.value(row_i))
                    });
                    vals.push(if bhcap866.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap866.value(row_i))
                    });
                    vals.push(if bhcap867.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap867.value(row_i))
                    });
                    vals.push(if bhcap868.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap868.value(row_i))
                    });
                    vals.push(if bhcap870.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap870.value(row_i))
                    });
                    vals.push(if bhcap872.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap872.value(row_i))
                    });
                    vals.push(if bhcap875.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcap875.value(row_i))
                    });
                    vals.push(if bhcaq257.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcaq257.value(row_i))
                    });
                    vals.push(if bhcaq258.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcaq258.value(row_i))
                    });
                    vals.push(if bhcas540.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcas540.value(row_i))
                    });
                    vals.push(if bhcb2210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcb2210.value(row_i))
                    });
                    vals.push(if bhcb2389.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcb2389.value(row_i))
                    });
                    vals.push(if bhcb2604.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcb2604.value(row_i))
                    });
                    vals.push(if bhcb3187.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcb3187.value(row_i))
                    });
                    vals.push(if bhcb6648.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcb6648.value(row_i))
                    });
                    vals.push(if bhcbhk29.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcbhk29.value(row_i))
                    });
                    vals.push(if bhcbj474.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcbj474.value(row_i))
                    });
                    vals.push(if bhce0010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce0010.value(row_i))
                    });
                    vals.push(if bhce1727.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce1727.value(row_i))
                    });
                    vals.push(if bhce1754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce1754.value(row_i))
                    });
                    vals.push(if bhce1773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce1773.value(row_i))
                    });
                    vals.push(if bhce2170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce2170.value(row_i))
                    });
                    vals.push(if bhce3123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce3123.value(row_i))
                    });
                    vals.push(if bhce3411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce3411.value(row_i))
                    });
                    vals.push(if bhce3429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce3429.value(row_i))
                    });
                    vals.push(if bhce3433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce3433.value(row_i))
                    });
                    vals.push(if bhce3545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce3545.value(row_i))
                    });
                    vals.push(if bhce5369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce5369.value(row_i))
                    });
                    vals.push(if bhce6566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce6566.value(row_i))
                    });
                    vals.push(if bhce6570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce6570.value(row_i))
                    });
                    vals.push(if bhce6572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhce6572.value(row_i))
                    });
                    vals.push(if bhcea167.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcea167.value(row_i))
                    });
                    vals.push(if bhcea250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcea250.value(row_i))
                    });
                    vals.push(if bhceb528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhceb528.value(row_i))
                    });
                    vals.push(if bhceb541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhceb541.value(row_i))
                    });
                    vals.push(if bhceb546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhceb546.value(row_i))
                    });
                    vals.push(if bhceb639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhceb639.value(row_i))
                    });
                    vals.push(if bhceb675.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhceb675.value(row_i))
                    });
                    vals.push(if bhceb681.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhceb681.value(row_i))
                    });
                    vals.push(if bhceg591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhceg591.value(row_i))
                    });
                    vals.push(if bhcm3531.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3531.value(row_i))
                    });
                    vals.push(if bhcm3532.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3532.value(row_i))
                    });
                    vals.push(if bhcm3533.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3533.value(row_i))
                    });
                    vals.push(if bhcm3534.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3534.value(row_i))
                    });
                    vals.push(if bhcm3535.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3535.value(row_i))
                    });
                    vals.push(if bhcm3536.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3536.value(row_i))
                    });
                    vals.push(if bhcm3537.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3537.value(row_i))
                    });
                    vals.push(if bhcm3541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3541.value(row_i))
                    });
                    vals.push(if bhcm3543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcm3543.value(row_i))
                    });
                    vals.push(if bhcp0010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0010.value(row_i))
                    });
                    vals.push(if bhcp0087.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0087.value(row_i))
                    });
                    vals.push(if bhcp0201.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0201.value(row_i))
                    });
                    vals.push(if bhcp0202.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0202.value(row_i))
                    });
                    vals.push(if bhcp0203.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0203.value(row_i))
                    });
                    vals.push(if bhcp0204.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0204.value(row_i))
                    });
                    vals.push(if bhcp0205.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0205.value(row_i))
                    });
                    vals.push(if bhcp0206.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0206.value(row_i))
                    });
                    vals.push(if bhcp0207.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0207.value(row_i))
                    });
                    vals.push(if bhcp0208.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0208.value(row_i))
                    });
                    vals.push(if bhcp0209.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0209.value(row_i))
                    });
                    vals.push(if bhcp0210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0210.value(row_i))
                    });
                    vals.push(if bhcp0277.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0277.value(row_i))
                    });
                    vals.push(if bhcp0279.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0279.value(row_i))
                    });
                    vals.push(if bhcp0362.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0362.value(row_i))
                    });
                    vals.push(if bhcp0363.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0363.value(row_i))
                    });
                    vals.push(if bhcp0364.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0364.value(row_i))
                    });
                    vals.push(if bhcp0365.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0365.value(row_i))
                    });
                    vals.push(if bhcp0368.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0368.value(row_i))
                    });
                    vals.push(if bhcp0400.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0400.value(row_i))
                    });
                    vals.push(if bhcp0416.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0416.value(row_i))
                    });
                    vals.push(if bhcp0447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0447.value(row_i))
                    });
                    vals.push(if bhcp0467.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0467.value(row_i))
                    });
                    vals.push(if bhcp0496.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0496.value(row_i))
                    });
                    vals.push(if bhcp0508.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0508.value(row_i))
                    });
                    vals.push(if bhcp0512.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0512.value(row_i))
                    });
                    vals.push(if bhcp0515.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0515.value(row_i))
                    });
                    vals.push(if bhcp0518.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0518.value(row_i))
                    });
                    vals.push(if bhcp0520.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0520.value(row_i))
                    });
                    vals.push(if bhcp0522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0522.value(row_i))
                    });
                    vals.push(if bhcp0533.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0533.value(row_i))
                    });
                    vals.push(if bhcp0534.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0534.value(row_i))
                    });
                    vals.push(if bhcp0536.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0536.value(row_i))
                    });
                    vals.push(if bhcp0537.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0537.value(row_i))
                    });
                    vals.push(if bhcp0538.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0538.value(row_i))
                    });
                    vals.push(if bhcp0539.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0539.value(row_i))
                    });
                    vals.push(if bhcp0540.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0540.value(row_i))
                    });
                    vals.push(if bhcp0541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0541.value(row_i))
                    });
                    vals.push(if bhcp0542.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0542.value(row_i))
                    });
                    vals.push(if bhcp0543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp0543.value(row_i))
                    });
                    vals.push(if bhcp1273.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1273.value(row_i))
                    });
                    vals.push(if bhcp1274.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1274.value(row_i))
                    });
                    vals.push(if bhcp1275.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1275.value(row_i))
                    });
                    vals.push(if bhcp1276.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1276.value(row_i))
                    });
                    vals.push(if bhcp1277.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1277.value(row_i))
                    });
                    vals.push(if bhcp1278.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1278.value(row_i))
                    });
                    vals.push(if bhcp1279.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1279.value(row_i))
                    });
                    vals.push(if bhcp1299.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1299.value(row_i))
                    });
                    vals.push(if bhcp1403.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1403.value(row_i))
                    });
                    vals.push(if bhcp1407.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1407.value(row_i))
                    });
                    vals.push(if bhcp1616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp1616.value(row_i))
                    });
                    vals.push(if bhcp2123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2123.value(row_i))
                    });
                    vals.push(if bhcp2125.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2125.value(row_i))
                    });
                    vals.push(if bhcp2145.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2145.value(row_i))
                    });
                    vals.push(if bhcp2160.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2160.value(row_i))
                    });
                    vals.push(if bhcp2165.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2165.value(row_i))
                    });
                    vals.push(if bhcp2170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2170.value(row_i))
                    });
                    vals.push(if bhcp2200.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2200.value(row_i))
                    });
                    vals.push(if bhcp2309.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2309.value(row_i))
                    });
                    vals.push(if bhcp2332.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2332.value(row_i))
                    });
                    vals.push(if bhcp2792.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2792.value(row_i))
                    });
                    vals.push(if bhcp2793.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2793.value(row_i))
                    });
                    vals.push(if bhcp2794.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2794.value(row_i))
                    });
                    vals.push(if bhcp2796.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2796.value(row_i))
                    });
                    vals.push(if bhcp2831.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2831.value(row_i))
                    });
                    vals.push(if bhcp2930.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp2930.value(row_i))
                    });
                    vals.push(if bhcp3123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3123.value(row_i))
                    });
                    vals.push(if bhcp3128.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3128.value(row_i))
                    });
                    vals.push(if bhcp3147.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3147.value(row_i))
                    });
                    vals.push(if bhcp3152.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3152.value(row_i))
                    });
                    vals.push(if bhcp3153.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3153.value(row_i))
                    });
                    vals.push(if bhcp3156.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3156.value(row_i))
                    });
                    vals.push(if bhcp3163.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3163.value(row_i))
                    });
                    vals.push(if bhcp3164.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3164.value(row_i))
                    });
                    vals.push(if bhcp3165.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3165.value(row_i))
                    });
                    vals.push(if bhcp3210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3210.value(row_i))
                    });
                    vals.push(if bhcp3230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3230.value(row_i))
                    });
                    vals.push(if bhcp3238.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3238.value(row_i))
                    });
                    vals.push(if bhcp3239.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3239.value(row_i))
                    });
                    vals.push(if bhcp3240.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3240.value(row_i))
                    });
                    vals.push(if bhcp3247.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3247.value(row_i))
                    });
                    vals.push(if bhcp3283.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3283.value(row_i))
                    });
                    vals.push(if bhcp3290.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3290.value(row_i))
                    });
                    vals.push(if bhcp3293.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3293.value(row_i))
                    });
                    vals.push(if bhcp3298.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3298.value(row_i))
                    });
                    vals.push(if bhcp3300.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3300.value(row_i))
                    });
                    vals.push(if bhcp3409.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3409.value(row_i))
                    });
                    vals.push(if bhcp3513.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3513.value(row_i))
                    });
                    vals.push(if bhcp3602.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3602.value(row_i))
                    });
                    vals.push(if bhcp3603.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3603.value(row_i))
                    });
                    vals.push(if bhcp3604.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3604.value(row_i))
                    });
                    vals.push(if bhcp3605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3605.value(row_i))
                    });
                    vals.push(if bhcp3606.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3606.value(row_i))
                    });
                    vals.push(if bhcp3607.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3607.value(row_i))
                    });
                    vals.push(if bhcp3609.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3609.value(row_i))
                    });
                    vals.push(if bhcp3611.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3611.value(row_i))
                    });
                    vals.push(if bhcp3612.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3612.value(row_i))
                    });
                    vals.push(if bhcp3613.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3613.value(row_i))
                    });
                    vals.push(if bhcp3614.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3614.value(row_i))
                    });
                    vals.push(if bhcp3615.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3615.value(row_i))
                    });
                    vals.push(if bhcp3616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3616.value(row_i))
                    });
                    vals.push(if bhcp3617.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3617.value(row_i))
                    });
                    vals.push(if bhcp3618.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3618.value(row_i))
                    });
                    vals.push(if bhcp3619.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp3619.value(row_i))
                    });
                    vals.push(if bhcp4000.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4000.value(row_i))
                    });
                    vals.push(if bhcp4062.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4062.value(row_i))
                    });
                    vals.push(if bhcp4073.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4073.value(row_i))
                    });
                    vals.push(if bhcp4091.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4091.value(row_i))
                    });
                    vals.push(if bhcp4130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4130.value(row_i))
                    });
                    vals.push(if bhcp4135.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4135.value(row_i))
                    });
                    vals.push(if bhcp4230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4230.value(row_i))
                    });
                    vals.push(if bhcp4243.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4243.value(row_i))
                    });
                    vals.push(if bhcp4250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4250.value(row_i))
                    });
                    vals.push(if bhcp4302.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4302.value(row_i))
                    });
                    vals.push(if bhcp4320.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4320.value(row_i))
                    });
                    vals.push(if bhcp4336.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4336.value(row_i))
                    });
                    vals.push(if bhcp4340.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4340.value(row_i))
                    });
                    vals.push(if bhcp4485.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4485.value(row_i))
                    });
                    vals.push(if bhcp4605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4605.value(row_i))
                    });
                    vals.push(if bhcp4635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4635.value(row_i))
                    });
                    vals.push(if bhcp4647.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4647.value(row_i))
                    });
                    vals.push(if bhcp4778.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp4778.value(row_i))
                    });
                    vals.push(if bhcp5485.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp5485.value(row_i))
                    });
                    vals.push(if bhcp5486.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp5486.value(row_i))
                    });
                    vals.push(if bhcp5487.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp5487.value(row_i))
                    });
                    vals.push(if bhcp5488.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp5488.value(row_i))
                    });
                    vals.push(if bhcp5489.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp5489.value(row_i))
                    });
                    vals.push(if bhcp5993.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp5993.value(row_i))
                    });
                    vals.push(if bhcp6552.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6552.value(row_i))
                    });
                    vals.push(if bhcp6567.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6567.value(row_i))
                    });
                    vals.push(if bhcp6571.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6571.value(row_i))
                    });
                    vals.push(if bhcp6573.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6573.value(row_i))
                    });
                    vals.push(if bhcp6588.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6588.value(row_i))
                    });
                    vals.push(if bhcp6589.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6589.value(row_i))
                    });
                    vals.push(if bhcp6590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6590.value(row_i))
                    });
                    vals.push(if bhcp6591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6591.value(row_i))
                    });
                    vals.push(if bhcp6592.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6592.value(row_i))
                    });
                    vals.push(if bhcp6596.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6596.value(row_i))
                    });
                    vals.push(if bhcp6600.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6600.value(row_i))
                    });
                    vals.push(if bhcp6604.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6604.value(row_i))
                    });
                    vals.push(if bhcp6607.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6607.value(row_i))
                    });
                    vals.push(if bhcp6619.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6619.value(row_i))
                    });
                    vals.push(if bhcp6649.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6649.value(row_i))
                    });
                    vals.push(if bhcp6741.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6741.value(row_i))
                    });
                    vals.push(if bhcp6742.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6742.value(row_i))
                    });
                    vals.push(if bhcp6743.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6743.value(row_i))
                    });
                    vals.push(if bhcp6744.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6744.value(row_i))
                    });
                    vals.push(if bhcp6758.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6758.value(row_i))
                    });
                    vals.push(if bhcp6773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6773.value(row_i))
                    });
                    vals.push(if bhcp6775.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6775.value(row_i))
                    });
                    vals.push(if bhcp6791.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6791.value(row_i))
                    });
                    vals.push(if bhcp6792.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6792.value(row_i))
                    });
                    vals.push(if bhcp6793.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6793.value(row_i))
                    });
                    vals.push(if bhcp6794.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6794.value(row_i))
                    });
                    vals.push(if bhcp6795.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp6795.value(row_i))
                    });
                    vals.push(if bhcp8434.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp8434.value(row_i))
                    });
                    vals.push(if bhcp8516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp8516.value(row_i))
                    });
                    vals.push(if bhcp8517.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp8517.value(row_i))
                    });
                    vals.push(if bhcp8518.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp8518.value(row_i))
                    });
                    vals.push(if bhcp8843.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp8843.value(row_i))
                    });
                    vals.push(if bhcp9191.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp9191.value(row_i))
                    });
                    vals.push(if bhcp9802.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcp9802.value(row_i))
                    });
                    vals.push(if bhcpa130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpa130.value(row_i))
                    });
                    vals.push(if bhcpb530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpb530.value(row_i))
                    });
                    vals.push(if bhcpc254.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpc254.value(row_i))
                    });
                    vals.push(if bhcpc255.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpc255.value(row_i))
                    });
                    vals.push(if bhcpc427.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpc427.value(row_i))
                    });
                    vals.push(if bhcpc428.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpc428.value(row_i))
                    });
                    vals.push(if bhcpc447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpc447.value(row_i))
                    });
                    vals.push(if bhcpf229.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpf229.value(row_i))
                    });
                    vals.push(if bhcpf737.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpf737.value(row_i))
                    });
                    vals.push(if bhcpf817.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpf817.value(row_i))
                    });
                    vals.push(if bhcpf818.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpf818.value(row_i))
                    });
                    vals.push(if bhcpf819.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpf819.value(row_i))
                    });
                    vals.push(if bhcpf820.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpf820.value(row_i))
                    });
                    vals.push(if bhcpf838.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpf838.value(row_i))
                    });
                    vals.push(if bhcpf841.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcpf841.value(row_i))
                    });
                    vals.push(if bhcpf842.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcpf842.value(row_i))
                    });
                    vals.push(if bhcpft28.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpft28.value(row_i))
                    });
                    vals.push(if bhcphk02.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcphk02.value(row_i))
                    });
                    vals.push(if bhcpht69.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpht69.value(row_i))
                    });
                    vals.push(if bhcpht70.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpht70.value(row_i))
                    });
                    vals.push(if bhcphu25.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcphu25.value(row_i))
                    });
                    vals.push(if bhcphu26.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcphu26.value(row_i))
                    });
                    vals.push(if bhcpj980.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpj980.value(row_i))
                    });
                    vals.push(if bhcpja22.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpja22.value(row_i))
                    });
                    vals.push(if bhcpjj33.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpjj33.value(row_i))
                    });
                    vals.push(if bhcpk297.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpk297.value(row_i))
                    });
                    vals.push(if bhcpky38.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcpky38.value(row_i))
                    });
                    vals.push(if bhcpm962.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcpm962.value(row_i))
                    });
                    vals.push(if bhct0426.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct0426.value(row_i))
                    });
                    vals.push(if bhct1754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct1754.value(row_i))
                    });
                    vals.push(if bhct1773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct1773.value(row_i))
                    });
                    vals.push(if bhct2143.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct2143.value(row_i))
                    });
                    vals.push(if bhct2150.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct2150.value(row_i))
                    });
                    vals.push(if bhct2160.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct2160.value(row_i))
                    });
                    vals.push(if bhct2170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct2170.value(row_i))
                    });
                    vals.push(if bhct2750.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct2750.value(row_i))
                    });
                    vals.push(if bhct3123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3123.value(row_i))
                    });
                    vals.push(if bhct3190.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3190.value(row_i))
                    });
                    vals.push(if bhct3210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3210.value(row_i))
                    });
                    vals.push(if bhct3247.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3247.value(row_i))
                    });
                    vals.push(if bhct3368.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3368.value(row_i))
                    });
                    vals.push(if bhct3411.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3411.value(row_i))
                    });
                    vals.push(if bhct3433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3433.value(row_i))
                    });
                    vals.push(if bhct3543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3543.value(row_i))
                    });
                    vals.push(if bhct3545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3545.value(row_i))
                    });
                    vals.push(if bhct3547.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3547.value(row_i))
                    });
                    vals.push(if bhct3548.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct3548.value(row_i))
                    });
                    vals.push(if bhct4230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct4230.value(row_i))
                    });
                    vals.push(if bhct4340.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct4340.value(row_i))
                    });
                    vals.push(if bhct4605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct4605.value(row_i))
                    });
                    vals.push(if bhct5369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct5369.value(row_i))
                    });
                    vals.push(if bhct5610.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct5610.value(row_i))
                    });
                    vals.push(if bhct6570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhct6570.value(row_i))
                    });
                    vals.push(if bhcta250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcta250.value(row_i))
                    });
                    vals.push(if bhctb528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhctb528.value(row_i))
                    });
                    vals.push(if bhctb590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhctb590.value(row_i))
                    });
                    vals.push(if bhctb591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhctb591.value(row_i))
                    });
                    vals.push(if bhcw3792.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcw3792.value(row_i))
                    });
                    vals.push(if bhcw5310.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcw5310.value(row_i))
                    });
                    vals.push(if bhcw5311.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcw5311.value(row_i))
                    });
                    vals.push(if bhcw7205.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcw7205.value(row_i))
                    });
                    vals.push(if bhcw7206.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcw7206.value(row_i))
                    });
                    vals.push(if bhcwa223.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwa223.value(row_i))
                    });
                    vals.push(if bhcwh311.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwh311.value(row_i))
                    });
                    vals.push(if bhcwkx78.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcwkx78.value(row_i))
                    });
                    vals.push(if bhcwkx83.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcwkx83.value(row_i))
                    });
                    vals.push(if bhcwle85.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwle85.value(row_i))
                    });
                    vals.push(if bhcwle86.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwle86.value(row_i))
                    });
                    vals.push(if bhcwle87.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwle87.value(row_i))
                    });
                    vals.push(if bhcwlf23.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwlf23.value(row_i))
                    });
                    vals.push(if bhcwlf24.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwlf24.value(row_i))
                    });
                    vals.push(if bhcwlf25.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwlf25.value(row_i))
                    });
                    vals.push(if bhcwmk66.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwmk66.value(row_i))
                    });
                    vals.push(if bhcwp793.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp793.value(row_i))
                    });
                    vals.push(if bhcwp851.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp851.value(row_i))
                    });
                    vals.push(if bhcwp852.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp852.value(row_i))
                    });
                    vals.push(if bhcwp853.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp853.value(row_i))
                    });
                    vals.push(if bhcwp854.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp854.value(row_i))
                    });
                    vals.push(if bhcwp855.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp855.value(row_i))
                    });
                    vals.push(if bhcwp856.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp856.value(row_i))
                    });
                    vals.push(if bhcwp857.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp857.value(row_i))
                    });
                    vals.push(if bhcwp858.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp858.value(row_i))
                    });
                    vals.push(if bhcwp859.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp859.value(row_i))
                    });
                    vals.push(if bhcwp870.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcwp870.value(row_i))
                    });
                    vals.push(if bhcx1754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcx1754.value(row_i))
                    });
                    vals.push(if bhcx1773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcx1773.value(row_i))
                    });
                    vals.push(if bhcx3123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcx3123.value(row_i))
                    });
                    vals.push(if bhcx3210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcx3210.value(row_i))
                    });
                    vals.push(if bhcx3368.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcx3368.value(row_i))
                    });
                    vals.push(if bhcx3545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcx3545.value(row_i))
                    });
                    vals.push(if bhcy1773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcy1773.value(row_i))
                    });
                    vals.push(if bhcy3123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcy3123.value(row_i))
                    });
                    vals.push(if bhcyja36.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcyja36.value(row_i))
                    });
                    vals.push(if bhdm1288.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1288.value(row_i))
                    });
                    vals.push(if bhdm1410.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1410.value(row_i))
                    });
                    vals.push(if bhdm1415.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1415.value(row_i))
                    });
                    vals.push(if bhdm1420.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1420.value(row_i))
                    });
                    vals.push(if bhdm1460.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1460.value(row_i))
                    });
                    vals.push(if bhdm1480.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1480.value(row_i))
                    });
                    vals.push(if bhdm1545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1545.value(row_i))
                    });
                    vals.push(if bhdm1564.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1564.value(row_i))
                    });
                    vals.push(if bhdm1590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1590.value(row_i))
                    });
                    vals.push(if bhdm1635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1635.value(row_i))
                    });
                    vals.push(if bhdm1755.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1755.value(row_i))
                    });
                    vals.push(if bhdm1766.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1766.value(row_i))
                    });
                    vals.push(if bhdm1797.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1797.value(row_i))
                    });
                    vals.push(if bhdm1975.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm1975.value(row_i))
                    });
                    vals.push(if bhdm2081.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm2081.value(row_i))
                    });
                    vals.push(if bhdm2122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm2122.value(row_i))
                    });
                    vals.push(if bhdm2123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm2123.value(row_i))
                    });
                    vals.push(if bhdm2165.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm2165.value(row_i))
                    });
                    vals.push(if bhdm3386.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3386.value(row_i))
                    });
                    vals.push(if bhdm3387.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3387.value(row_i))
                    });
                    vals.push(if bhdm3465.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3465.value(row_i))
                    });
                    vals.push(if bhdm3466.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3466.value(row_i))
                    });
                    vals.push(if bhdm3516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3516.value(row_i))
                    });
                    vals.push(if bhdm3545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3545.value(row_i))
                    });
                    vals.push(if bhdm3546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3546.value(row_i))
                    });
                    vals.push(if bhdm3547.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3547.value(row_i))
                    });
                    vals.push(if bhdm3548.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm3548.value(row_i))
                    });
                    vals.push(if bhdm5367.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm5367.value(row_i))
                    });
                    vals.push(if bhdm5368.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm5368.value(row_i))
                    });
                    vals.push(if bhdm6631.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm6631.value(row_i))
                    });
                    vals.push(if bhdm6636.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdm6636.value(row_i))
                    });
                    vals.push(if bhdma164.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdma164.value(row_i))
                    });
                    vals.push(if bhdma242.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdma242.value(row_i))
                    });
                    vals.push(if bhdma243.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdma243.value(row_i))
                    });
                    vals.push(if bhdmb561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmb561.value(row_i))
                    });
                    vals.push(if bhdmb562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmb562.value(row_i))
                    });
                    vals.push(if bhdmb987.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmb987.value(row_i))
                    });
                    vals.push(if bhdmb993.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmb993.value(row_i))
                    });
                    vals.push(if bhdmf560.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf560.value(row_i))
                    });
                    vals.push(if bhdmf576.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf576.value(row_i))
                    });
                    vals.push(if bhdmf577.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf577.value(row_i))
                    });
                    vals.push(if bhdmf578.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf578.value(row_i))
                    });
                    vals.push(if bhdmf579.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf579.value(row_i))
                    });
                    vals.push(if bhdmf580.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf580.value(row_i))
                    });
                    vals.push(if bhdmf581.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf581.value(row_i))
                    });
                    vals.push(if bhdmf582.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf582.value(row_i))
                    });
                    vals.push(if bhdmf583.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf583.value(row_i))
                    });
                    vals.push(if bhdmf584.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf584.value(row_i))
                    });
                    vals.push(if bhdmf585.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf585.value(row_i))
                    });
                    vals.push(if bhdmf586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf586.value(row_i))
                    });
                    vals.push(if bhdmf587.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf587.value(row_i))
                    });
                    vals.push(if bhdmf588.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf588.value(row_i))
                    });
                    vals.push(if bhdmf589.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf589.value(row_i))
                    });
                    vals.push(if bhdmf590.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf590.value(row_i))
                    });
                    vals.push(if bhdmf591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf591.value(row_i))
                    });
                    vals.push(if bhdmf592.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf592.value(row_i))
                    });
                    vals.push(if bhdmf593.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf593.value(row_i))
                    });
                    vals.push(if bhdmf594.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf594.value(row_i))
                    });
                    vals.push(if bhdmf595.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf595.value(row_i))
                    });
                    vals.push(if bhdmf596.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf596.value(row_i))
                    });
                    vals.push(if bhdmf597.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf597.value(row_i))
                    });
                    vals.push(if bhdmf598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf598.value(row_i))
                    });
                    vals.push(if bhdmf599.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf599.value(row_i))
                    });
                    vals.push(if bhdmf600.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf600.value(row_i))
                    });
                    vals.push(if bhdmf601.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf601.value(row_i))
                    });
                    vals.push(if bhdmf604.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf604.value(row_i))
                    });
                    vals.push(if bhdmf605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf605.value(row_i))
                    });
                    vals.push(if bhdmf606.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf606.value(row_i))
                    });
                    vals.push(if bhdmf607.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf607.value(row_i))
                    });
                    vals.push(if bhdmf611.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf611.value(row_i))
                    });
                    vals.push(if bhdmf612.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf612.value(row_i))
                    });
                    vals.push(if bhdmf613.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf613.value(row_i))
                    });
                    vals.push(if bhdmf614.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf614.value(row_i))
                    });
                    vals.push(if bhdmf615.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf615.value(row_i))
                    });
                    vals.push(if bhdmf616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf616.value(row_i))
                    });
                    vals.push(if bhdmf617.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf617.value(row_i))
                    });
                    vals.push(if bhdmf618.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf618.value(row_i))
                    });
                    vals.push(if bhdmf624.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf624.value(row_i))
                    });
                    vals.push(if bhdmf625.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf625.value(row_i))
                    });
                    vals.push(if bhdmf626.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf626.value(row_i))
                    });
                    vals.push(if bhdmf627.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf627.value(row_i))
                    });
                    vals.push(if bhdmf628.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf628.value(row_i))
                    });
                    vals.push(if bhdmf629.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf629.value(row_i))
                    });
                    vals.push(if bhdmf630.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf630.value(row_i))
                    });
                    vals.push(if bhdmf631.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf631.value(row_i))
                    });
                    vals.push(if bhdmf632.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf632.value(row_i))
                    });
                    vals.push(if bhdmf633.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf633.value(row_i))
                    });
                    vals.push(if bhdmf634.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf634.value(row_i))
                    });
                    vals.push(if bhdmf635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf635.value(row_i))
                    });
                    vals.push(if bhdmf636.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf636.value(row_i))
                    });
                    vals.push(if bhdmf639.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf639.value(row_i))
                    });
                    vals.push(if bhdmf640.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf640.value(row_i))
                    });
                    vals.push(if bhdmf670.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf670.value(row_i))
                    });
                    vals.push(if bhdmf671.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf671.value(row_i))
                    });
                    vals.push(if bhdmf672.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf672.value(row_i))
                    });
                    vals.push(if bhdmf673.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf673.value(row_i))
                    });
                    vals.push(if bhdmf674.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf674.value(row_i))
                    });
                    vals.push(if bhdmf675.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf675.value(row_i))
                    });
                    vals.push(if bhdmf676.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf676.value(row_i))
                    });
                    vals.push(if bhdmf677.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf677.value(row_i))
                    });
                    vals.push(if bhdmf678.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf678.value(row_i))
                    });
                    vals.push(if bhdmf679.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf679.value(row_i))
                    });
                    vals.push(if bhdmf680.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf680.value(row_i))
                    });
                    vals.push(if bhdmf681.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf681.value(row_i))
                    });
                    vals.push(if bhdmf724.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmf724.value(row_i))
                    });
                    vals.push(if bhdmg209.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg209.value(row_i))
                    });
                    vals.push(if bhdmg210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg210.value(row_i))
                    });
                    vals.push(if bhdmg211.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg211.value(row_i))
                    });
                    vals.push(if bhdmg299.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg299.value(row_i))
                    });
                    vals.push(if bhdmg332.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg332.value(row_i))
                    });
                    vals.push(if bhdmg333.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg333.value(row_i))
                    });
                    vals.push(if bhdmg334.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg334.value(row_i))
                    });
                    vals.push(if bhdmg335.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg335.value(row_i))
                    });
                    vals.push(if bhdmg379.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg379.value(row_i))
                    });
                    vals.push(if bhdmg380.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg380.value(row_i))
                    });
                    vals.push(if bhdmg381.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg381.value(row_i))
                    });
                    vals.push(if bhdmg382.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg382.value(row_i))
                    });
                    vals.push(if bhdmg383.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg383.value(row_i))
                    });
                    vals.push(if bhdmg384.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg384.value(row_i))
                    });
                    vals.push(if bhdmg385.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg385.value(row_i))
                    });
                    vals.push(if bhdmg386.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg386.value(row_i))
                    });
                    vals.push(if bhdmg387.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg387.value(row_i))
                    });
                    vals.push(if bhdmg388.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg388.value(row_i))
                    });
                    vals.push(if bhdmg651.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg651.value(row_i))
                    });
                    vals.push(if bhdmg652.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmg652.value(row_i))
                    });
                    vals.push(if bhdmhk06.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmhk06.value(row_i))
                    });
                    vals.push(if bhdmhk31.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmhk31.value(row_i))
                    });
                    vals.push(if bhdmhk32.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmhk32.value(row_i))
                    });
                    vals.push(if bhdmj451.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmj451.value(row_i))
                    });
                    vals.push(if bhdmj454.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmj454.value(row_i))
                    });
                    vals.push(if bhdmk045.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk045.value(row_i))
                    });
                    vals.push(if bhdmk046.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk046.value(row_i))
                    });
                    vals.push(if bhdmk047.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk047.value(row_i))
                    });
                    vals.push(if bhdmk048.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk048.value(row_i))
                    });
                    vals.push(if bhdmk049.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk049.value(row_i))
                    });
                    vals.push(if bhdmk050.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk050.value(row_i))
                    });
                    vals.push(if bhdmk051.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk051.value(row_i))
                    });
                    vals.push(if bhdmk052.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk052.value(row_i))
                    });
                    vals.push(if bhdmk053.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk053.value(row_i))
                    });
                    vals.push(if bhdmk054.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk054.value(row_i))
                    });
                    vals.push(if bhdmk055.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk055.value(row_i))
                    });
                    vals.push(if bhdmk056.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk056.value(row_i))
                    });
                    vals.push(if bhdmk057.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk057.value(row_i))
                    });
                    vals.push(if bhdmk058.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk058.value(row_i))
                    });
                    vals.push(if bhdmk059.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk059.value(row_i))
                    });
                    vals.push(if bhdmk060.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk060.value(row_i))
                    });
                    vals.push(if bhdmk061.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk061.value(row_i))
                    });
                    vals.push(if bhdmk062.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk062.value(row_i))
                    });
                    vals.push(if bhdmk063.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk063.value(row_i))
                    });
                    vals.push(if bhdmk064.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk064.value(row_i))
                    });
                    vals.push(if bhdmk065.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk065.value(row_i))
                    });
                    vals.push(if bhdmk066.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk066.value(row_i))
                    });
                    vals.push(if bhdmk067.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk067.value(row_i))
                    });
                    vals.push(if bhdmk068.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk068.value(row_i))
                    });
                    vals.push(if bhdmk069.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk069.value(row_i))
                    });
                    vals.push(if bhdmk070.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk070.value(row_i))
                    });
                    vals.push(if bhdmk071.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk071.value(row_i))
                    });
                    vals.push(if bhdmk105.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk105.value(row_i))
                    });
                    vals.push(if bhdmk106.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk106.value(row_i))
                    });
                    vals.push(if bhdmk107.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk107.value(row_i))
                    });
                    vals.push(if bhdmk108.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk108.value(row_i))
                    });
                    vals.push(if bhdmk109.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk109.value(row_i))
                    });
                    vals.push(if bhdmk110.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk110.value(row_i))
                    });
                    vals.push(if bhdmk111.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk111.value(row_i))
                    });
                    vals.push(if bhdmk112.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk112.value(row_i))
                    });
                    vals.push(if bhdmk113.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk113.value(row_i))
                    });
                    vals.push(if bhdmk114.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk114.value(row_i))
                    });
                    vals.push(if bhdmk115.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk115.value(row_i))
                    });
                    vals.push(if bhdmk116.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk116.value(row_i))
                    });
                    vals.push(if bhdmk117.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk117.value(row_i))
                    });
                    vals.push(if bhdmk118.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk118.value(row_i))
                    });
                    vals.push(if bhdmk119.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk119.value(row_i))
                    });
                    vals.push(if bhdmk130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk130.value(row_i))
                    });
                    vals.push(if bhdmk131.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk131.value(row_i))
                    });
                    vals.push(if bhdmk132.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk132.value(row_i))
                    });
                    vals.push(if bhdmk158.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk158.value(row_i))
                    });
                    vals.push(if bhdmk159.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk159.value(row_i))
                    });
                    vals.push(if bhdmk160.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk160.value(row_i))
                    });
                    vals.push(if bhdmk161.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk161.value(row_i))
                    });
                    vals.push(if bhdmk162.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk162.value(row_i))
                    });
                    vals.push(if bhdmk166.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk166.value(row_i))
                    });
                    vals.push(if bhdmk169.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk169.value(row_i))
                    });
                    vals.push(if bhdmk170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk170.value(row_i))
                    });
                    vals.push(if bhdmk171.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk171.value(row_i))
                    });
                    vals.push(if bhdmk172.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk172.value(row_i))
                    });
                    vals.push(if bhdmk173.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk173.value(row_i))
                    });
                    vals.push(if bhdmk174.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk174.value(row_i))
                    });
                    vals.push(if bhdmk175.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk175.value(row_i))
                    });
                    vals.push(if bhdmk176.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk176.value(row_i))
                    });
                    vals.push(if bhdmk177.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk177.value(row_i))
                    });
                    vals.push(if bhdmk187.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk187.value(row_i))
                    });
                    vals.push(if bhdmk188.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk188.value(row_i))
                    });
                    vals.push(if bhdmk189.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk189.value(row_i))
                    });
                    vals.push(if bhdmk190.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk190.value(row_i))
                    });
                    vals.push(if bhdmk191.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk191.value(row_i))
                    });
                    vals.push(if bhdmk195.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk195.value(row_i))
                    });
                    vals.push(if bhdmk196.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk196.value(row_i))
                    });
                    vals.push(if bhdmk197.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk197.value(row_i))
                    });
                    vals.push(if bhdmk198.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk198.value(row_i))
                    });
                    vals.push(if bhdmk199.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk199.value(row_i))
                    });
                    vals.push(if bhdmk200.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk200.value(row_i))
                    });
                    vals.push(if bhdmk208.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk208.value(row_i))
                    });
                    vals.push(if bhdmk209.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk209.value(row_i))
                    });
                    vals.push(if bhdmk210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk210.value(row_i))
                    });
                    vals.push(if bhdmk211.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmk211.value(row_i))
                    });
                    vals.push(if bhdmkx57.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhdmkx57.value(row_i))
                    });
                    vals.push(if bhfn3360.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhfn3360.value(row_i))
                    });
                    vals.push(if bhfn3543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhfn3543.value(row_i))
                    });
                    vals.push(if bhfn6631.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhfn6631.value(row_i))
                    });
                    vals.push(if bhfn6636.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhfn6636.value(row_i))
                    });
                    vals.push(if bhfna245.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhfna245.value(row_i))
                    });
                    vals.push(if bhfnk260.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhfnk260.value(row_i))
                    });
                    vals.push(if bhod2389.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhod2389.value(row_i))
                    });
                    vals.push(if bhod2604.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhod2604.value(row_i))
                    });
                    vals.push(if bhod3187.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhod3187.value(row_i))
                    });
                    vals.push(if bhod3189.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhod3189.value(row_i))
                    });
                    vals.push(if bhod6648.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhod6648.value(row_i))
                    });
                    vals.push(if bhodhk29.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhodhk29.value(row_i))
                    });
                    vals.push(if bhodj474.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhodj474.value(row_i))
                    });
                    vals.push(if bhpa0365.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhpa0365.value(row_i))
                    });
                    vals.push(if bhpa4340.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhpa4340.value(row_i))
                    });
                    vals.push(if bhpx8901.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhpx8901.value(row_i).into())
                    });
                    vals.push(if bhsp0010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0010.value(row_i))
                    });
                    vals.push(if bhsp0027.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0027.value(row_i))
                    });
                    vals.push(if bhsp0087.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0087.value(row_i))
                    });
                    vals.push(if bhsp0088.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0088.value(row_i))
                    });
                    vals.push(if bhsp0089.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0089.value(row_i))
                    });
                    vals.push(if bhsp0201.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0201.value(row_i))
                    });
                    vals.push(if bhsp0202.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0202.value(row_i))
                    });
                    vals.push(if bhsp0206.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0206.value(row_i))
                    });
                    vals.push(if bhsp0390.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0390.value(row_i))
                    });
                    vals.push(if bhsp0416.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0416.value(row_i))
                    });
                    vals.push(if bhsp0447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0447.value(row_i))
                    });
                    vals.push(if bhsp0496.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0496.value(row_i))
                    });
                    vals.push(if bhsp0508.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0508.value(row_i))
                    });
                    vals.push(if bhsp0523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0523.value(row_i))
                    });
                    vals.push(if bhsp0530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp0530.value(row_i))
                    });
                    vals.push(if bhsp1283.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp1283.value(row_i))
                    });
                    vals.push(if bhsp2111.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2111.value(row_i))
                    });
                    vals.push(if bhsp2112.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2112.value(row_i))
                    });
                    vals.push(if bhsp2122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2122.value(row_i))
                    });
                    vals.push(if bhsp2145.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2145.value(row_i))
                    });
                    vals.push(if bhsp2148.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2148.value(row_i))
                    });
                    vals.push(if bhsp2170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2170.value(row_i))
                    });
                    vals.push(if bhsp2309.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2309.value(row_i))
                    });
                    vals.push(if bhsp2723.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2723.value(row_i))
                    });
                    vals.push(if bhsp2724.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2724.value(row_i))
                    });
                    vals.push(if bhsp2792.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2792.value(row_i))
                    });
                    vals.push(if bhsp2794.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2794.value(row_i))
                    });
                    vals.push(if bhsp2796.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2796.value(row_i))
                    });
                    vals.push(if bhsp2932.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp2932.value(row_i))
                    });
                    vals.push(if bhsp3049.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3049.value(row_i))
                    });
                    vals.push(if bhsp3066.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3066.value(row_i))
                    });
                    vals.push(if bhsp3123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3123.value(row_i))
                    });
                    vals.push(if bhsp3148.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3148.value(row_i))
                    });
                    vals.push(if bhsp3151.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3151.value(row_i))
                    });
                    vals.push(if bhsp3152.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3152.value(row_i))
                    });
                    vals.push(if bhsp3153.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3153.value(row_i))
                    });
                    vals.push(if bhsp3154.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3154.value(row_i))
                    });
                    vals.push(if bhsp3155.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3155.value(row_i))
                    });
                    vals.push(if bhsp3156.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3156.value(row_i))
                    });
                    vals.push(if bhsp3158.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3158.value(row_i))
                    });
                    vals.push(if bhsp3166.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3166.value(row_i))
                    });
                    vals.push(if bhsp3167.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3167.value(row_i))
                    });
                    vals.push(if bhsp3210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3210.value(row_i))
                    });
                    vals.push(if bhsp3230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3230.value(row_i))
                    });
                    vals.push(if bhsp3238.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3238.value(row_i))
                    });
                    vals.push(if bhsp3239.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3239.value(row_i))
                    });
                    vals.push(if bhsp3247.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3247.value(row_i))
                    });
                    vals.push(if bhsp3283.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3283.value(row_i))
                    });
                    vals.push(if bhsp3300.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3300.value(row_i))
                    });
                    vals.push(if bhsp3513.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3513.value(row_i))
                    });
                    vals.push(if bhsp3523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3523.value(row_i))
                    });
                    vals.push(if bhsp3524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3524.value(row_i))
                    });
                    vals.push(if bhsp3525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3525.value(row_i))
                    });
                    vals.push(if bhsp3526.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3526.value(row_i))
                    });
                    vals.push(if bhsp3527.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3527.value(row_i))
                    });
                    vals.push(if bhsp3605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3605.value(row_i))
                    });
                    vals.push(if bhsp3620.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3620.value(row_i))
                    });
                    vals.push(if bhsp3621.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp3621.value(row_i))
                    });
                    vals.push(if bhsp4000.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4000.value(row_i))
                    });
                    vals.push(if bhsp4073.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4073.value(row_i))
                    });
                    vals.push(if bhsp4093.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4093.value(row_i))
                    });
                    vals.push(if bhsp4130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4130.value(row_i))
                    });
                    vals.push(if bhsp4250.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4250.value(row_i))
                    });
                    vals.push(if bhsp4302.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4302.value(row_i))
                    });
                    vals.push(if bhsp4336.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4336.value(row_i))
                    });
                    vals.push(if bhsp4340.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4340.value(row_i))
                    });
                    vals.push(if bhsp4778.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp4778.value(row_i))
                    });
                    vals.push(if bhsp5993.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp5993.value(row_i))
                    });
                    vals.push(if bhsp6416.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp6416.value(row_i))
                    });
                    vals.push(if bhsp6649.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp6649.value(row_i))
                    });
                    vals.push(if bhsp6796.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp6796.value(row_i))
                    });
                    vals.push(if bhsp6797.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp6797.value(row_i))
                    });
                    vals.push(if bhsp8434.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8434.value(row_i))
                    });
                    vals.push(if bhsp8516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8516.value(row_i))
                    });
                    vals.push(if bhsp8517.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8517.value(row_i))
                    });
                    vals.push(if bhsp8519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8519.value(row_i))
                    });
                    vals.push(if bhsp8520.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8520.value(row_i))
                    });
                    vals.push(if bhsp8521.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8521.value(row_i))
                    });
                    vals.push(if bhsp8522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8522.value(row_i))
                    });
                    vals.push(if bhsp8523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8523.value(row_i))
                    });
                    vals.push(if bhsp8524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8524.value(row_i))
                    });
                    vals.push(if bhsp8525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8525.value(row_i))
                    });
                    vals.push(if bhsp8526.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8526.value(row_i))
                    });
                    vals.push(if bhsp8527.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8527.value(row_i))
                    });
                    vals.push(if bhsp8528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8528.value(row_i))
                    });
                    vals.push(if bhsp8529.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8529.value(row_i))
                    });
                    vals.push(if bhsp8530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8530.value(row_i))
                    });
                    vals.push(if bhsp8843.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp8843.value(row_i))
                    });
                    vals.push(if bhsp9191.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp9191.value(row_i))
                    });
                    vals.push(if bhsp9802.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhsp9802.value(row_i))
                    });
                    vals.push(if bhspa024.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspa024.value(row_i))
                    });
                    vals.push(if bhspa130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspa130.value(row_i))
                    });
                    vals.push(if bhspa530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspa530.value(row_i))
                    });
                    vals.push(if bhspb530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspb530.value(row_i))
                    });
                    vals.push(if bhspc009.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc009.value(row_i))
                    });
                    vals.push(if bhspc159.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc159.value(row_i))
                    });
                    vals.push(if bhspc160.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspc160.value(row_i))
                    });
                    vals.push(if bhspc161.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc161.value(row_i))
                    });
                    vals.push(if bhspc252.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc252.value(row_i))
                    });
                    vals.push(if bhspc253.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc253.value(row_i))
                    });
                    vals.push(if bhspc254.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc254.value(row_i))
                    });
                    vals.push(if bhspc255.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc255.value(row_i))
                    });
                    vals.push(if bhspc256.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc256.value(row_i))
                    });
                    vals.push(if bhspc257.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc257.value(row_i))
                    });
                    vals.push(if bhspc427.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc427.value(row_i))
                    });
                    vals.push(if bhspc428.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc428.value(row_i))
                    });
                    vals.push(if bhspc447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc447.value(row_i))
                    });
                    vals.push(if bhspc700.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc700.value(row_i))
                    });
                    vals.push(if bhspc701.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc701.value(row_i))
                    });
                    vals.push(if bhspc702.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc702.value(row_i))
                    });
                    vals.push(if bhspc884.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspc884.value(row_i))
                    });
                    vals.push(if bhspf074.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspf074.value(row_i))
                    });
                    vals.push(if bhspf075.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspf075.value(row_i))
                    });
                    vals.push(if bhspf229.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspf229.value(row_i))
                    });
                    vals.push(if bhspf819.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspf819.value(row_i))
                    });
                    vals.push(if bhspf820.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspf820.value(row_i))
                    });
                    vals.push(if bhspf838.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspf838.value(row_i))
                    });
                    vals.push(if bhspf841.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspf841.value(row_i))
                    });
                    vals.push(if bhspf842.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspf842.value(row_i))
                    });
                    vals.push(if bhspft28.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspft28.value(row_i))
                    });
                    vals.push(if bhspft42.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspft42.value(row_i))
                    });
                    vals.push(if bhspft43.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspft43.value(row_i))
                    });
                    vals.push(if bhspft44.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspft44.value(row_i))
                    });
                    vals.push(if bhspg234.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspg234.value(row_i))
                    });
                    vals.push(if bhspg235.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspg235.value(row_i))
                    });
                    vals.push(if bhspht69.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspht69.value(row_i))
                    });
                    vals.push(if bhspht70.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspht70.value(row_i))
                    });
                    vals.push(if bhspht95.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspht95.value(row_i))
                    });
                    vals.push(if bhspj980.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspj980.value(row_i))
                    });
                    vals.push(if bhspk141.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspk141.value(row_i))
                    });
                    vals.push(if bhspky38.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspky38.value(row_i))
                    });
                    vals.push(if bhspm962.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspm962.value(row_i))
                    });
                    vals.push(if bhspmz36.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhspmz36.value(row_i))
                    });
                    vals.push(if bhspnk60.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhspnk60.value(row_i))
                    });
                    vals.push(if bhsx8901.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhsx8901.value(row_i).into())
                    });
                    vals.push(if bhtxf655.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxf655.value(row_i).into())
                    });
                    vals.push(if bhtxf656.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxf656.value(row_i).into())
                    });
                    vals.push(if bhtxf657.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxf657.value(row_i).into())
                    });
                    vals.push(if bhtxf658.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxf658.value(row_i).into())
                    });
                    vals.push(if bhtxf659.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxf659.value(row_i).into())
                    });
                    vals.push(if bhtxf660.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxf660.value(row_i).into())
                    });
                    vals.push(if bhtxg546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxg546.value(row_i).into())
                    });
                    vals.push(if bhtxg551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxg551.value(row_i).into())
                    });
                    vals.push(if bhtxg556.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxg556.value(row_i).into())
                    });
                    vals.push(if bhtxg561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxg561.value(row_i).into())
                    });
                    vals.push(if bhtxg571.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxg571.value(row_i).into())
                    });
                    vals.push(if bhtxg576.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhtxg576.value(row_i))
                    });
                    vals.push(if bhtxg581.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxg581.value(row_i).into())
                    });
                    vals.push(if bhtxg586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(bhtxg586.value(row_i).into())
                    });
                    vals.push(if rssd4087.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd4087.value(row_i).into())
                    });
                    vals.push(if rssd6191.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd6191.value(row_i))
                    });
                    vals.push(if rssd9001.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9001.value(row_i))
                    });
                    vals.push(if rssd9005.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9005.value(row_i).into())
                    });
                    vals.push(if rssd9007.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Date(rssd9007.value(row_i))
                    });
                    vals.push(if rssd9008.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Date(rssd9008.value(row_i))
                    });
                    vals.push(if rssd9010.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9010.value(row_i).into())
                    });
                    vals.push(if rssd9014.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9014.value(row_i))
                    });
                    vals.push(if rssd9016.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9016.value(row_i))
                    });
                    vals.push(if rssd9017.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9017.value(row_i).into())
                    });
                    vals.push(if rssd9028.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9028.value(row_i).into())
                    });
                    vals.push(if rssd9029.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9029.value(row_i).into())
                    });
                    vals.push(if rssd9030.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9030.value(row_i))
                    });
                    vals.push(if rssd9031.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9031.value(row_i))
                    });
                    vals.push(if rssd9032.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9032.value(row_i))
                    });
                    vals.push(if rssd9037.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9037.value(row_i))
                    });
                    vals.push(if rssd9038.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9038.value(row_i).into())
                    });
                    vals.push(if rssd9039.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9039.value(row_i))
                    });
                    vals.push(if rssd9042.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9042.value(row_i))
                    });
                    vals.push(if rssd9044.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9044.value(row_i))
                    });
                    vals.push(if rssd9045.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9045.value(row_i))
                    });
                    vals.push(if rssd9046.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9046.value(row_i))
                    });
                    vals.push(if rssd9047.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9047.value(row_i))
                    });
                    vals.push(if rssd9048.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9048.value(row_i))
                    });
                    vals.push(if rssd9049.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9049.value(row_i))
                    });
                    vals.push(if rssd9050.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9050.value(row_i))
                    });
                    vals.push(if rssd9052.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9052.value(row_i))
                    });
                    vals.push(if rssd9053.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9053.value(row_i))
                    });
                    vals.push(if rssd9054.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9054.value(row_i))
                    });
                    vals.push(if rssd9055.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9055.value(row_i))
                    });
                    vals.push(if rssd9056.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9056.value(row_i))
                    });
                    vals.push(if rssd9059.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9059.value(row_i))
                    });
                    vals.push(if rssd9060.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9060.value(row_i))
                    });
                    vals.push(if rssd9061.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9061.value(row_i))
                    });
                    vals.push(if rssd9101.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9101.value(row_i).into())
                    });
                    vals.push(if rssd9130.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9130.value(row_i).into())
                    });
                    vals.push(if rssd9132.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9132.value(row_i))
                    });
                    vals.push(if rssd9138.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9138.value(row_i))
                    });
                    vals.push(if rssd9146.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9146.value(row_i))
                    });
                    vals.push(if rssd9150.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9150.value(row_i))
                    });
                    vals.push(if rssd9161.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9161.value(row_i).into())
                    });
                    vals.push(if rssd9170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9170.value(row_i))
                    });
                    vals.push(if rssd9192.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9192.value(row_i).into())
                    });
                    vals.push(if rssd9198.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9198.value(row_i))
                    });
                    vals.push(if rssd9200.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9200.value(row_i).into())
                    });
                    vals.push(if rssd9210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9210.value(row_i))
                    });
                    vals.push(if rssd9213.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9213.value(row_i))
                    });
                    vals.push(if rssd9216.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9216.value(row_i))
                    });
                    vals.push(if rssd9220.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(rssd9220.value(row_i).into())
                    });
                    vals.push(if rssd9320.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9320.value(row_i))
                    });
                    vals.push(if rssd9374.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9374.value(row_i))
                    });
                    vals.push(if rssd9375.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9375.value(row_i))
                    });
                    vals.push(if rssd9421.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9421.value(row_i))
                    });
                    vals.push(if rssd9422.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9422.value(row_i))
                    });
                    vals.push(if rssd9424.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9424.value(row_i))
                    });
                    vals.push(if rssd9425.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9425.value(row_i))
                    });
                    vals.push(if rssd9579.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9579.value(row_i))
                    });
                    vals.push(if rssd9950.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Date(rssd9950.value(row_i))
                    });
                    vals.push(if rssd9955.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9955.value(row_i))
                    });
                    vals.push(if rssd9999.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(rssd9999.value(row_i))
                    });
                    vals.push(if texc3573.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc3573.value(row_i))
                    });
                    vals.push(if texc3575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc3575.value(row_i))
                    });
                    vals.push(if texc6373.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc6373.value(row_i))
                    });
                    vals.push(if texc6561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6561.value(row_i))
                    });
                    vals.push(if texc6562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6562.value(row_i))
                    });
                    vals.push(if texc6568.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6568.value(row_i))
                    });
                    vals.push(if texc6586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6586.value(row_i))
                    });
                    vals.push(if texc6995.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6995.value(row_i))
                    });
                    vals.push(if texc6996.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6996.value(row_i))
                    });
                    vals.push(if texc6997.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6997.value(row_i))
                    });
                    vals.push(if texc6998.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc6998.value(row_i))
                    });
                    vals.push(if texc8520.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8520.value(row_i))
                    });
                    vals.push(if texc8521.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc8521.value(row_i))
                    });
                    vals.push(if texc8522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc8522.value(row_i))
                    });
                    vals.push(if texc8523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8523.value(row_i))
                    });
                    vals.push(if texc8524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8524.value(row_i))
                    });
                    vals.push(if texc8525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(texc8525.value(row_i))
                    });
                    vals.push(if texc8557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8557.value(row_i))
                    });
                    vals.push(if texc8558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8558.value(row_i))
                    });
                    vals.push(if texc8559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8559.value(row_i))
                    });
                    vals.push(if texc8562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8562.value(row_i))
                    });
                    vals.push(if texc8563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8563.value(row_i))
                    });
                    vals.push(if texc8564.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8564.value(row_i))
                    });
                    vals.push(if texc8565.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8565.value(row_i))
                    });
                    vals.push(if texc8566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8566.value(row_i))
                    });
                    vals.push(if texc8567.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(texc8567.value(row_i))
                    });
                    vals.push(if text3571.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text3571.value(row_i).into())
                    });
                    vals.push(if text3573.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text3573.value(row_i).into())
                    });
                    vals.push(if text3575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text3575.value(row_i).into())
                    });
                    vals.push(if text4769.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(text4769.value(row_i))
                    });
                    vals.push(if text5351.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5351.value(row_i).into())
                    });
                    vals.push(if text5352.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5352.value(row_i).into())
                    });
                    vals.push(if text5353.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5353.value(row_i).into())
                    });
                    vals.push(if text5354.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5354.value(row_i).into())
                    });
                    vals.push(if text5355.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5355.value(row_i).into())
                    });
                    vals.push(if text5356.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5356.value(row_i).into())
                    });
                    vals.push(if text5357.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5357.value(row_i).into())
                    });
                    vals.push(if text5358.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5358.value(row_i).into())
                    });
                    vals.push(if text5359.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5359.value(row_i).into())
                    });
                    vals.push(if text5360.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5360.value(row_i).into())
                    });
                    vals.push(if text5485.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5485.value(row_i).into())
                    });
                    vals.push(if text5486.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5486.value(row_i).into())
                    });
                    vals.push(if text5487.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5487.value(row_i).into())
                    });
                    vals.push(if text5488.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5488.value(row_i).into())
                    });
                    vals.push(if text5489.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text5489.value(row_i).into())
                    });
                    vals.push(if text5523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(text5523.value(row_i))
                    });
                    vals.push(if text6373.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text6373.value(row_i).into())
                    });
                    vals.push(if text6561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text6561.value(row_i).into())
                    });
                    vals.push(if text6562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text6562.value(row_i).into())
                    });
                    vals.push(if text6568.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text6568.value(row_i).into())
                    });
                    vals.push(if text6586.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text6586.value(row_i).into())
                    });
                    vals.push(if text6995.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(text6995.value(row_i))
                    });
                    vals.push(if text6996.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(text6996.value(row_i))
                    });
                    vals.push(if text6997.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(text6997.value(row_i))
                    });
                    vals.push(if text6998.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(text6998.value(row_i))
                    });
                    vals.push(if text8520.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8520.value(row_i).into())
                    });
                    vals.push(if text8521.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8521.value(row_i).into())
                    });
                    vals.push(if text8522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8522.value(row_i).into())
                    });
                    vals.push(if text8523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8523.value(row_i).into())
                    });
                    vals.push(if text8524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8524.value(row_i).into())
                    });
                    vals.push(if text8525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8525.value(row_i).into())
                    });
                    vals.push(if text8526.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8526.value(row_i).into())
                    });
                    vals.push(if text8527.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8527.value(row_i).into())
                    });
                    vals.push(if text8528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8528.value(row_i).into())
                    });
                    vals.push(if text8529.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8529.value(row_i).into())
                    });
                    vals.push(if text8530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8530.value(row_i).into())
                    });
                    vals.push(if text8557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8557.value(row_i).into())
                    });
                    vals.push(if text8558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8558.value(row_i).into())
                    });
                    vals.push(if text8559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8559.value(row_i).into())
                    });
                    vals.push(if text8562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8562.value(row_i).into())
                    });
                    vals.push(if text8563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8563.value(row_i).into())
                    });
                    vals.push(if text8564.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8564.value(row_i).into())
                    });
                    vals.push(if text8565.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8565.value(row_i).into())
                    });
                    vals.push(if text8566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8566.value(row_i).into())
                    });
                    vals.push(if text8567.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(text8567.value(row_i).into())
                    });
                    vals.push(if textb027.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb027.value(row_i).into())
                    });
                    vals.push(if textb028.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb028.value(row_i).into())
                    });
                    vals.push(if textb029.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb029.value(row_i).into())
                    });
                    vals.push(if textb030.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb030.value(row_i).into())
                    });
                    vals.push(if textb031.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb031.value(row_i).into())
                    });
                    vals.push(if textb032.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb032.value(row_i).into())
                    });
                    vals.push(if textb033.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb033.value(row_i).into())
                    });
                    vals.push(if textb034.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb034.value(row_i).into())
                    });
                    vals.push(if textb035.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb035.value(row_i).into())
                    });
                    vals.push(if textb036.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb036.value(row_i).into())
                    });
                    vals.push(if textb037.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb037.value(row_i).into())
                    });
                    vals.push(if textb038.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb038.value(row_i).into())
                    });
                    vals.push(if textb039.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb039.value(row_i).into())
                    });
                    vals.push(if textb040.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb040.value(row_i).into())
                    });
                    vals.push(if textb041.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb041.value(row_i).into())
                    });
                    vals.push(if textb042.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb042.value(row_i).into())
                    });
                    vals.push(if textb043.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb043.value(row_i).into())
                    });
                    vals.push(if textb044.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb044.value(row_i).into())
                    });
                    vals.push(if textb045.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb045.value(row_i).into())
                    });
                    vals.push(if textb046.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb046.value(row_i).into())
                    });
                    vals.push(if textb047.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb047.value(row_i).into())
                    });
                    vals.push(if textb048.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb048.value(row_i).into())
                    });
                    vals.push(if textb049.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb049.value(row_i).into())
                    });
                    vals.push(if textb050.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb050.value(row_i).into())
                    });
                    vals.push(if textb051.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb051.value(row_i).into())
                    });
                    vals.push(if textb052.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb052.value(row_i).into())
                    });
                    vals.push(if textb053.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb053.value(row_i).into())
                    });
                    vals.push(if textb054.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(textb054.value(row_i))
                    });
                    vals.push(if textb055.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(textb055.value(row_i))
                    });
                    vals.push(if textb056.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textb056.value(row_i).into())
                    });
                    vals.push(if textc231.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textc231.value(row_i).into())
                    });
                    vals.push(if textc490.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(textc490.value(row_i))
                    });
                    vals.push(if textc497.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textc497.value(row_i).into())
                    });
                    vals.push(if textc703.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textc703.value(row_i).into())
                    });
                    vals.push(if textc708.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textc708.value(row_i).into())
                    });
                    vals.push(if textc714.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textc714.value(row_i).into())
                    });
                    vals.push(if textc715.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textc715.value(row_i).into())
                    });
                    vals.push(if textft29.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(textft29.value(row_i))
                    });
                    vals.push(if textft31.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::StringOwned(textft31.value(row_i).into())
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
