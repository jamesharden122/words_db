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
pub struct BhckSeries2 {
    pub bhck0383: Option<f64>,
    pub bhck0384: Option<f64>,
    pub bhck0387: Option<f64>,
    pub bhck0416: Option<f64>,
    pub bhck0535: Option<f64>,
    pub bhck1395: Option<f64>,
    pub bhck1403: Option<f64>,
    pub bhck1406: Option<f64>,
    pub bhck1407: Option<f64>,
    pub bhck1658: Option<f64>,
    pub bhck1659: Option<f64>,
    pub bhck1661: Option<f64>,
    pub bhck1771: Option<f64>,
    pub bhck1772: Option<f64>,
    pub bhck1914: Option<f64>,
    pub bhck2033: Option<f64>,
    pub bhck2079: Option<f64>,
    pub bhck2122: Option<f64>,
    pub bhck2123: Option<f64>,
    pub bhck2125: Option<f64>,
    pub bhck2145: Option<f64>,
    pub bhck2170: Option<f64>,
    pub bhck2221: Option<f64>,
    pub bhck2419: Option<f64>,
    pub bhck2432: Option<f64>,
    pub bhck2635: Option<f64>,
    pub bhck2744: Option<f64>,
    pub bhck2948: Option<f64>,
    pub bhck3196: Option<f64>,
    pub bhck3210: Option<f64>,
    pub bhck3240: Option<f64>,
    pub bhck3247: Option<f64>,
    pub bhck3283: Option<f64>,
    pub bhck3290: Option<f64>,
    pub bhck3293: Option<f64>,
    pub bhck3300: Option<f64>,
    pub bhck3353: Option<f64>,
    pub bhck3365: Option<f64>,
    pub bhck3368: Option<f64>,
    pub bhck3376: Option<f64>,
    pub bhck3377: Option<f64>,
    pub bhck3378: Option<f64>,
    pub bhck3401: Option<f64>,
    pub bhck3402: Option<f64>,
    pub bhck3404: Option<f64>,
    pub bhck3408: Option<f64>,
    pub bhck3428: Option<f64>,
    pub bhck3429: Option<f64>,
    pub bhck3432: Option<f64>,
    pub bhck3433: Option<f64>,
    pub bhck3459: Option<f64>,
    pub bhck3515: Option<f64>,
    pub bhck3516: Option<f64>,
    pub bhck3517: Option<f64>,
    pub bhck3519: Option<f64>,
    pub bhck3521: Option<f64>,
    pub bhck3531: Option<f64>,
    pub bhck3532: Option<f64>,
    pub bhck3533: Option<f64>,
    pub bhck3534: Option<f64>,
    pub bhck3535: Option<f64>,
    pub bhck3536: Option<f64>,
    pub bhck3537: Option<f64>,
    pub bhck3542: Option<f64>,
    pub bhck3543: Option<f64>,
    pub bhck3545: Option<f64>,
    pub bhck3547: Option<f64>,
    pub bhck3548: Option<f64>,
    pub bhck3573: Option<f64>,
    pub bhck3575: Option<f64>,
    pub bhck3577: Option<f64>,
    pub bhck3579: Option<f64>,
    pub bhck3583: Option<f64>,
    pub bhck3585: Option<f64>,
    pub bhck3589: Option<f64>,
    pub bhck3591: Option<f64>,
    pub bhck3792: Option<f64>,
    pub bhck3814: Option<f64>,
    pub bhck3815: Option<f64>,
    pub bhck3817: Option<f64>,
    pub bhck3818: Option<f64>,
    pub bhck4062: Option<f64>,
    pub bhck4073: Option<f64>,
    pub bhck4079: Option<f64>,
    pub bhck4093: Option<f64>,
    pub bhck4107: Option<f64>,
    pub bhck4135: Option<f64>,
    pub bhck4230: Option<f64>,
    pub bhck4243: Option<f64>,
    pub bhck4307: Option<f64>,
    pub bhck4483: Option<f64>,
    pub bhck4505: Option<f64>,
    pub bhck4605: Option<f64>,
    pub bhck4617: Option<f64>,
    pub bhck4618: Option<f64>,
    pub bhck4627: Option<f64>,
    pub bhck4628: Option<f64>,
    pub bhck4661: Option<f64>,
    pub bhck4662: Option<f64>,
    pub bhck4663: Option<f64>,
    pub bhck4664: Option<f64>,
    pub bhck4665: Option<f64>,
    pub bhck4666: Option<f64>,
    pub bhck4667: Option<f64>,
    pub bhck4668: Option<f64>,
    pub bhck4669: Option<f64>,
    pub bhck4782: Option<f64>,
    pub bhck4783: Option<f64>,
    pub bhck5306: Option<f64>,
    pub bhck5311: Option<f64>,
    pub bhck5352: Option<f64>,
    pub bhck5353: Option<f64>,
    pub bhck5357: Option<f64>,
    pub bhck5358: Option<f64>,
    pub bhck5376: Option<f64>,
    pub bhck5396: Option<f64>,
    pub bhck5410: Option<f64>,
    pub bhck5412: Option<f64>,
    pub bhck5414: Option<f64>,
    pub bhck5479: Option<f64>,
    pub bhck5483: Option<f64>,
    pub bhck5484: Option<f64>,
    pub bhck5500: Option<f64>,
    pub bhck5501: Option<f64>,
    pub bhck5502: Option<f64>,
    pub bhck5503: Option<f64>,
    pub bhck5504: Option<f64>,
    pub bhck5505: Option<f64>,
    pub bhck5523: Option<f64>,
    pub bhck5524: Option<f64>,
    pub bhck5525: Option<f64>,
    pub bhck5526: Option<f64>,
    pub bhck5990: Option<f64>,
    pub bhck6562: Option<f64>,
    pub bhck6568: Option<f64>,
    pub bhck6570: Option<f64>,
    pub bhck6577: Option<f64>,
    pub bhck6996: Option<bool>,
    pub bhck6997: Option<bool>,
    pub bhck7204: Option<f64>,
    pub bhck7205: Option<f64>,
    pub bhck7206: Option<f64>,
    pub bhck8274: Option<f64>,
    pub bhck8275: Option<f64>,
    pub bhck8551: Option<f64>,
    pub bhck8552: Option<f64>,
    pub bhck8553: Option<f64>,
    pub bhck8554: Option<f64>,
    pub bhck8555: Option<f64>,
    pub bhck8556: Option<f64>,
    pub bhck8701: Option<f64>,
    pub bhck8702: Option<f64>,
    pub bhck8703: Option<f64>,
    pub bhck8704: Option<f64>,
    pub bhck8705: Option<f64>,
    pub bhck8706: Option<f64>,
    pub bhck8707: Option<f64>,
    pub bhck8708: Option<f64>,
    pub bhck8709: Option<f64>,
    pub bhck8710: Option<f64>,
    pub bhck8711: Option<f64>,
    pub bhck8712: Option<f64>,
    pub bhck8713: Option<f64>,
    pub bhck8714: Option<f64>,
    pub bhck8715: Option<f64>,
    pub bhck8716: Option<f64>,
    pub bhck8717: Option<bool>,
    pub bhck8718: Option<bool>,
    pub bhck8723: Option<f64>,
    pub bhck8724: Option<f64>,
    pub bhck8725: Option<f64>,
    pub bhck8726: Option<f64>,
    pub bhck8727: Option<f64>,
    pub bhck8728: Option<f64>,
    pub bhck8729: Option<f64>,
    pub bhck8730: Option<f64>,
    pub bhck8731: Option<f64>,
    pub bhck8732: Option<f64>,
    pub bhck8765: Option<f64>,
    pub bhck8768: Option<bool>,
    pub bhck8784: Option<f64>,
    pub bhck8834: Option<bool>,
    pub bhck8836: Option<bool>,
    pub bhck8838: Option<bool>,
    pub bhck9191: Option<f64>,
    pub bhck9802: Option<f64>,
    pub bhcka102: Option<f64>,
    pub bhcka120: Option<bool>,
    pub bhcka121: Option<bool>,
    pub bhcka122: Option<bool>,
    pub bhcka123: Option<bool>,
    pub bhcka124: Option<bool>,
    pub bhcka126: Option<f64>,
    pub bhcka127: Option<f64>,
    pub bhcka128: Option<bool>,
    pub bhcka195: Option<f64>,
    pub bhcka220: Option<f64>,
    pub bhcka223: Option<f64>,
    pub bhcka249: Option<f64>,
    pub bhcka288: Option<f64>,
    pub bhcka591: Option<f64>,
    pub bhckb027: Option<f64>,
    pub bhckb028: Option<f64>,
    pub bhckb031: Option<f64>,
    pub bhckb033: Option<f64>,
    pub bhckb034: Option<f64>,
    pub bhckb037: Option<f64>,
    pub bhckb038: Option<f64>,
    pub bhckb041: Option<f64>,
    pub bhckb042: Option<f64>,
    pub bhckb043: Option<f64>,
    pub bhckb046: Option<f64>,
    pub bhckb048: Option<f64>,
    pub bhckb049: Option<f64>,
    pub bhckb052: Option<f64>,
    pub bhckb053: Option<f64>,
    pub bhckb056: Option<f64>,
    pub bhckb491: Option<f64>,
    pub bhckb507: Option<f64>,
    pub bhckb513: Option<f64>,
    pub bhckb515: Option<f64>,
    pub bhckb517: Option<f64>,
    pub bhckb541: Option<f64>,
    pub bhckb558: Option<f64>,
    pub bhckb589: Option<f64>,
    pub bhckb696: Option<f64>,
    pub bhckb697: Option<f64>,
    pub bhckb698: Option<f64>,
    pub bhckb699: Option<f64>,
    pub bhckb700: Option<f64>,
    pub bhckb701: Option<f64>,
    pub bhckb702: Option<f64>,
    pub bhckb703: Option<f64>,
    pub bhckb704: Option<f64>,
    pub bhckb705: Option<f64>,
    pub bhckb706: Option<f64>,
    pub bhckb707: Option<f64>,
    pub bhckb708: Option<f64>,
    pub bhckb709: Option<f64>,
    pub bhckb710: Option<f64>,
    pub bhckb711: Option<f64>,
    pub bhckb712: Option<f64>,
    pub bhckb713: Option<f64>,
    pub bhckb714: Option<f64>,
    pub bhckb715: Option<f64>,
    pub bhckb716: Option<f64>,
    pub bhckb717: Option<f64>,
    pub bhckb718: Option<f64>,
    pub bhckb719: Option<f64>,
    pub bhckb720: Option<f64>,
    pub bhckb721: Option<f64>,
    pub bhckb722: Option<f64>,
    pub bhckb723: Option<f64>,
    pub bhckb724: Option<f64>,
    pub bhckb725: Option<f64>,
    pub bhckb726: Option<f64>,
    pub bhckb727: Option<f64>,
    pub bhckb728: Option<f64>,
    pub bhckb729: Option<f64>,
    pub bhckb730: Option<f64>,
    pub bhckb731: Option<f64>,
    pub bhckb732: Option<f64>,
    pub bhckb733: Option<f64>,
    pub bhckb734: Option<f64>,
    pub bhckb735: Option<f64>,
    pub bhckb736: Option<f64>,
    pub bhckb737: Option<f64>,
    pub bhckb738: Option<f64>,
    pub bhckb739: Option<f64>,
    pub bhckb740: Option<f64>,
    pub bhckb741: Option<f64>,
    pub bhckb742: Option<f64>,
    pub bhckb743: Option<f64>,
    pub bhckb744: Option<f64>,
    pub bhckb745: Option<f64>,
    pub bhckb746: Option<f64>,
    pub bhckb754: Option<f64>,
    pub bhckb755: Option<f64>,
    pub bhckb756: Option<f64>,
    pub bhckb757: Option<f64>,
    pub bhckb758: Option<f64>,
    pub bhckb759: Option<f64>,
    pub bhckb760: Option<f64>,
    pub bhckb764: Option<f64>,
    pub bhckb765: Option<f64>,
    pub bhckb766: Option<f64>,
    pub bhckb767: Option<f64>,
    pub bhckb768: Option<f64>,
    pub bhckb769: Option<f64>,
    pub bhckb773: Option<f64>,
    pub bhckb774: Option<f64>,
    pub bhckb775: Option<f64>,
    pub bhckb783: Option<f64>,
    pub bhckb784: Option<f64>,
    pub bhckb785: Option<f64>,
    pub bhckb786: Option<f64>,
    pub bhckb787: Option<f64>,
    pub bhckb788: Option<f64>,
    pub bhckb789: Option<f64>,
    pub bhckb804: Option<f64>,
    pub bhckb805: Option<f64>,
    pub bhckb808: Option<f64>,
    pub bhckb809: Option<f64>,
    pub bhckb982: Option<f64>,
    pub bhckb989: Option<f64>,
    pub bhckb995: Option<f64>,
    pub bhckb997: Option<f64>,
    pub bhckc015: Option<f64>,
    pub bhckc018: Option<f64>,
    pub bhckc026: Option<f64>,
    pub bhckc027: Option<f64>,
    pub bhckc217: Option<f64>,
    pub bhckc218: Option<f64>,
    pub bhckc227: Option<f64>,
    pub bhckc242: Option<f64>,
    pub bhckc244: Option<f64>,
    pub bhckc245: Option<f64>,
    pub bhckc247: Option<f64>,
    pub bhckc248: Option<f64>,
    pub bhckc249: Option<f64>,
    pub bhckc388: Option<f64>,
    pub bhckc389: Option<f64>,
    pub bhckc391: Option<f64>,
    pub bhckc393: Option<f64>,
    pub bhckc394: Option<f64>,
    pub bhckc395: Option<f64>,
    pub bhckc396: Option<f64>,
    pub bhckc397: Option<f64>,
    pub bhckc398: Option<f64>,
    pub bhckc399: Option<f64>,
    pub bhckc400: Option<f64>,
    pub bhckc401: Option<f64>,
    pub bhckc402: Option<f64>,
    pub bhckc403: Option<f64>,
    pub bhckc404: Option<f64>,
    pub bhckc405: Option<f64>,
    pub bhckc406: Option<f64>,
    pub bhckc407: Option<f64>,
    pub bhckc408: Option<f64>,
    pub bhckc409: Option<f64>,
    pub bhckc502: Option<f64>,
    pub bhckc699: Option<f64>,
    pub bhckc779: Option<f64>,
    pub bhckc780: Option<f64>,
    pub bhckc866: Option<f64>,
    pub bhckc867: Option<f64>,
    pub bhckc868: Option<f64>,
    pub bhckd957: Option<f64>,
    pub bhckd961: Option<f64>,
    pub bhckd966: Option<f64>,
    pub bhckd976: Option<f64>,
    pub bhckd977: Option<f64>,
    pub bhckd978: Option<f64>,
    pub bhckd979: Option<f64>,
    pub bhckd980: Option<f64>,
    pub bhckd981: Option<f64>,
    pub bhckd987: Option<f64>,
    pub bhckd988: Option<f64>,
    pub bhckd989: Option<f64>,
    pub bhckd990: Option<f64>,
    pub bhckd997: Option<f64>,
    pub bhckd998: Option<f64>,
    pub bhckd999: Option<f64>,
    pub bhckf064: Option<f64>,
    pub bhckf065: Option<f64>,
    pub bhckf066: Option<f64>,
    pub bhckf067: Option<f64>,
    pub bhckf068: Option<f64>,
    pub bhckf069: Option<f64>,
    pub bhckf186: Option<f64>,
    pub bhckf187: Option<f64>,
    pub bhckf188: Option<f64>,
    pub bhckf230: Option<f64>,
    pub bhckf231: Option<f64>,
    pub bhckf232: Option<f64>,
    pub bhckf240: Option<f64>,
    pub bhckf243: Option<f64>,
    pub bhckf246: Option<f64>,
    pub bhckf249: Option<f64>,
    pub bhckf252: Option<f64>,
    pub bhckf255: Option<f64>,
    pub bhckf258: Option<f64>,
    pub bhckf261: Option<f64>,
    pub bhckf559: Option<f64>,
    pub bhckf597: Option<f64>,
    pub bhckf598: Option<f64>,
    pub bhckf599: Option<f64>,
    pub bhckf600: Option<f64>,
    pub bhckf601: Option<f64>,
    pub bhckf609: Option<f64>,
    pub bhckf610: Option<f64>,
    pub bhckf614: Option<f64>,
    pub bhckf615: Option<f64>,
    pub bhckf616: Option<f64>,
    pub bhckf617: Option<f64>,
    pub bhckf618: Option<f64>,
    pub bhckf624: Option<f64>,
    pub bhckf632: Option<f64>,
    pub bhckf633: Option<f64>,
    pub bhckf634: Option<f64>,
    pub bhckf635: Option<f64>,
    pub bhckf636: Option<f64>,
    pub bhckf641: Option<f64>,
    pub bhckf642: Option<f64>,
    pub bhckf643: Option<f64>,
    pub bhckf644: Option<f64>,
    pub bhckf645: Option<f64>,
    pub bhckf646: Option<f64>,
    pub bhckf647: Option<f64>,
    pub bhckf648: Option<f64>,
    pub bhckf649: Option<f64>,
    pub bhckf650: Option<f64>,
    pub bhckf651: Option<f64>,
    pub bhckf652: Option<f64>,
    pub bhckf653: Option<f64>,
    pub bhckf654: Option<f64>,
    pub bhckf656: Option<f64>,
    pub bhckf657: Option<f64>,
    pub bhckf659: Option<f64>,
    pub bhckf660: Option<f64>,
    pub bhckf667: Option<f64>,
    pub bhckf668: Option<f64>,
    pub bhckf669: Option<f64>,
    pub bhckf699: Option<f64>,
    pub bhckf790: Option<f64>,
    pub bhckf837: Option<f64>,
    pub bhckf838: Option<f64>,
    pub bhckf842: Option<bool>,
    pub bhckft04: Option<f64>,
    pub bhckft05: Option<f64>,
    pub bhckg105: Option<f64>,
    pub bhckg214: Option<f64>,
    pub bhckg215: Option<f64>,
    pub bhckg216: Option<f64>,
    pub bhckg217: Option<f64>,
    pub bhckg219: Option<f64>,
    pub bhckg220: Option<f64>,
    pub bhckg222: Option<f64>,
    pub bhckg299: Option<f64>,
    pub bhckg332: Option<f64>,
    pub bhckg333: Option<f64>,
    pub bhckg334: Option<f64>,
    pub bhckg335: Option<f64>,
    pub bhckg348: Option<f64>,
    pub bhckg349: Option<f64>,
    pub bhckg350: Option<f64>,
    pub bhckg351: Option<f64>,
    pub bhckg352: Option<f64>,
    pub bhckg353: Option<f64>,
    pub bhckg354: Option<f64>,
    pub bhckg355: Option<f64>,
    pub bhckg356: Option<f64>,
    pub bhckg357: Option<f64>,
    pub bhckg358: Option<f64>,
    pub bhckg359: Option<f64>,
    pub bhckg360: Option<f64>,
    pub bhckg361: Option<f64>,
    pub bhckg362: Option<f64>,
    pub bhckg363: Option<f64>,
    pub bhckg364: Option<f64>,
    pub bhckg365: Option<f64>,
    pub bhckg366: Option<f64>,
    pub bhckg367: Option<f64>,
    pub bhckg368: Option<f64>,
    pub bhckg369: Option<f64>,
    pub bhckg370: Option<f64>,
    pub bhckg371: Option<f64>,
    pub bhckg372: Option<f64>,
    pub bhckg373: Option<f64>,
    pub bhckg374: Option<f64>,
    pub bhckg375: Option<f64>,
    pub bhckg378: Option<f64>,
    pub bhckg379: Option<f64>,
    pub bhckg380: Option<f64>,
    pub bhckg381: Option<f64>,
    pub bhckg382: Option<f64>,
    pub bhckg383: Option<f64>,
    pub bhckg384: Option<f64>,
    pub bhckg385: Option<f64>,
    pub bhckg386: Option<f64>,
    pub bhckg387: Option<f64>,
    pub bhckg388: Option<f64>,
    pub bhckg418: Option<f64>,
    pub bhckg419: Option<f64>,
    pub bhckg420: Option<f64>,
    pub bhckg421: Option<f64>,
    pub bhckg422: Option<f64>,
    pub bhckg423: Option<f64>,
    pub bhckg424: Option<f64>,
    pub bhckg425: Option<f64>,
    pub bhckg426: Option<f64>,
    pub bhckg427: Option<f64>,
    pub bhckg428: Option<f64>,
    pub bhckg429: Option<f64>,
    pub bhckg430: Option<f64>,
    pub bhckg431: Option<f64>,
    pub bhckg432: Option<f64>,
    pub bhckg433: Option<f64>,
    pub bhckg434: Option<f64>,
    pub bhckg435: Option<f64>,
    pub bhckg436: Option<f64>,
    pub bhckg437: Option<f64>,
    pub bhckg438: Option<f64>,
    pub bhckg439: Option<f64>,
    pub bhckg440: Option<f64>,
    pub bhckg441: Option<f64>,
    pub bhckg442: Option<f64>,
    pub bhckg443: Option<f64>,
    pub bhckg444: Option<f64>,
    pub bhckg445: Option<f64>,
    pub bhckg446: Option<f64>,
    pub bhckg447: Option<f64>,
    pub bhckg448: Option<f64>,
    pub bhckg449: Option<f64>,
    pub bhckg450: Option<f64>,
    pub bhckg451: Option<f64>,
    pub bhckg452: Option<f64>,
    pub bhckg453: Option<f64>,
    pub bhckg454: Option<f64>,
    pub bhckg455: Option<f64>,
    pub bhckg456: Option<f64>,
    pub bhckg457: Option<f64>,
    pub bhckg458: Option<f64>,
    pub bhckg459: Option<f64>,
    pub bhckg460: Option<f64>,
    pub bhckg461: Option<f64>,
    pub bhckg462: Option<f64>,
    pub bhckg493: Option<f64>,
    pub bhckg494: Option<f64>,
    pub bhckg495: Option<f64>,
    pub bhckg496: Option<f64>,
    pub bhckg497: Option<f64>,
    pub bhckg498: Option<f64>,
    pub bhckg499: Option<f64>,
    pub bhckg500: Option<f64>,
    pub bhckg501: Option<f64>,
    pub bhckg502: Option<f64>,
    pub bhckg503: Option<f64>,
    pub bhckg504: Option<f64>,
    pub bhckg505: Option<f64>,
    pub bhckg506: Option<f64>,
    pub bhckg512: Option<f64>,
    pub bhckg513: Option<f64>,
    pub bhckg514: Option<f64>,
    pub bhckg515: Option<f64>,
    pub bhckg516: Option<f64>,
    pub bhckg517: Option<f64>,
    pub bhckg518: Option<f64>,
    pub bhckg519: Option<f64>,
    pub bhckg520: Option<f64>,
    pub bhckg526: Option<f64>,
    pub bhckg527: Option<f64>,
    pub bhckg528: Option<f64>,
    pub bhckg529: Option<f64>,
    pub bhckg530: Option<f64>,
    pub bhckg531: Option<f64>,
    pub bhckg532: Option<f64>,
    pub bhckg533: Option<f64>,
    pub bhckg534: Option<f64>,
    pub bhckg535: Option<f64>,
    pub bhckg551: Option<f64>,
    pub bhckg552: Option<f64>,
    pub bhckg553: Option<f64>,
    pub bhckg554: Option<f64>,
    pub bhckg555: Option<f64>,
    pub bhckg556: Option<f64>,
    pub bhckg557: Option<f64>,
    pub bhckg558: Option<f64>,
    pub bhckg559: Option<f64>,
    pub bhckg560: Option<f64>,
    pub bhckg576: Option<f64>,
    pub bhckg577: Option<f64>,
    pub bhckg578: Option<f64>,
    pub bhckg579: Option<f64>,
    pub bhckg580: Option<f64>,
    pub bhckg581: Option<f64>,
    pub bhckg582: Option<f64>,
    pub bhckg583: Option<f64>,
    pub bhckg584: Option<f64>,
    pub bhckg585: Option<f64>,
    pub bhckg591: Option<f64>,
    pub bhckg603: Option<f64>,
    pub bhckg604: Option<f64>,
    pub bhckg605: Option<f64>,
    pub bhckg612: Option<f64>,
    pub bhckg613: Option<f64>,
    pub bhckg614: Option<f64>,
    pub bhckg615: Option<f64>,
    pub bhckg616: Option<f64>,
    pub bhckg617: Option<f64>,
    pub bhckg624: Option<f64>,
    pub bhckg625: Option<f64>,
    pub bhckg626: Option<f64>,
    pub bhckg627: Option<f64>,
    pub bhckg628: Option<f64>,
    pub bhckg629: Option<f64>,
    pub bhckg630: Option<f64>,
    pub bhckg631: Option<f64>,
    pub bhckg632: Option<f64>,
    pub bhckg633: Option<f64>,
    pub bhckg634: Option<f64>,
    pub bhckg635: Option<f64>,
    pub bhckg636: Option<f64>,
    pub bhckg637: Option<f64>,
    pub bhckg641: Option<f64>,
    pub bhckg651: Option<f64>,
    pub bhckg652: Option<f64>,
    pub bhckh171: Option<f64>,
    pub bhckh191: Option<f64>,
    pub bhckh289: Option<f64>,
    pub bhckh290: Option<f64>,
    pub bhckh291: Option<f64>,
    pub bhckh292: Option<f64>,
    pub bhckh300: Option<f64>,
    pub bhckh301: Option<f64>,
    pub bhckh302: Option<f64>,
    pub bhckh303: Option<f64>,
    pub bhckh304: Option<f64>,
    pub bhckh307: Option<f64>,
    pub bhckh308: Option<f64>,
    pub bhckh309: Option<f64>,
    pub bhckh310: Option<f64>,
    pub bhckhj74: Option<f64>,
    pub bhckhj75: Option<f64>,
    pub bhckhj76: Option<f64>,
    pub bhckhj77: Option<f64>,
    pub bhckhj86: Option<f64>,
    pub bhckhj87: Option<f64>,
    pub bhckhj90: Option<f64>,
    pub bhckhj91: Option<f64>,
    pub bhckhj96: Option<f64>,
    pub bhckhj97: Option<f64>,
    pub bhckhj98: Option<f64>,
    pub bhckhj99: Option<f64>,
    pub bhckhk00: Option<f64>,
    pub bhckhk01: Option<f64>,
    pub bhckhk25: Option<f64>,
    pub bhckhk26: Option<f64>,
    pub bhckhk27: Option<f64>,
    pub bhckhk28: Option<f64>,
    pub bhckht50: Option<f64>,
    pub bhckht51: Option<f64>,
    pub bhckht52: Option<f64>,
    pub bhckht53: Option<f64>,
    pub bhckht66: Option<f64>,
    pub bhckht67: Option<f64>,
    pub bhckht68: Option<f64>,
    pub bhckht70: Option<f64>,
    pub bhckht81: Option<f64>,
    pub bhckht82: Option<f64>,
    pub bhckht86: Option<f64>,
    pub bhckhu16: Option<f64>,
    pub bhckhu17: Option<f64>,
    pub bhckhu18: Option<f64>,
    pub bhckj319: Option<f64>,
    pub bhckj321: Option<f64>,
    pub bhckj457: Option<f64>,
    pub bhckj458: Option<f64>,
    pub bhckj459: Option<f64>,
    pub bhckjf77: Option<f64>,
    pub bhckjf78: Option<f64>,
    pub bhckjh89: Option<f64>,
    pub bhckjh90: Option<f64>,
    pub bhckjh95: Option<f64>,
    pub bhckjh96: Option<f64>,
    pub bhckjj02: Option<f64>,
    pub bhckjj33: Option<f64>,
    pub bhckk042: Option<f64>,
    pub bhckk043: Option<f64>,
    pub bhckk044: Option<f64>,
    pub bhckk102: Option<f64>,
    pub bhckk103: Option<f64>,
    pub bhckk104: Option<f64>,
    pub bhckk133: Option<f64>,
    pub bhckk141: Option<f64>,
    pub bhckk195: Option<f64>,
    pub bhckk197: Option<f64>,
    pub bhckk198: Option<f64>,
    pub bhckk199: Option<f64>,
    pub bhckk200: Option<f64>,
    pub bhckk206: Option<f64>,
    pub bhckk209: Option<f64>,
    pub bhckk210: Option<f64>,
    pub bhckk211: Option<f64>,
    pub bhckkx48: Option<f64>,
    pub bhckkx49: Option<f64>,
    pub bhckkx56: Option<f64>,
    pub bhckkx59: Option<f64>,
    pub bhckkx66: Option<f64>,
    pub bhckkx67: Option<f64>,
    pub bhckkx68: Option<f64>,
    pub bhckl183: Option<f64>,
    pub bhckl184: Option<f64>,
    pub bhckl185: Option<f64>,
    pub bhckl186: Option<f64>,
    pub bhckl187: Option<f64>,
    pub bhckl188: Option<f64>,
    pub bhckl191: Option<bool>,
    pub bhckl192: Option<bool>,
    pub bhckle75: Option<f64>,
    pub bhcklg25: Option<bool>,
    pub bhcklg27: Option<f64>,
    pub bhcklg28: Option<f64>,
    pub bhckll57: Option<f64>,
    pub bhckm288: Option<f64>,
    pub bhckm708: Option<f64>,
    pub bhckm709: Option<f64>,
    pub bhckm710: Option<f64>,
    pub bhckm711: Option<f64>,
    pub bhckm712: Option<f64>,
    pub bhckm713: Option<f64>,
    pub bhckm714: Option<f64>,
    pub bhckm715: Option<f64>,
    pub bhckm716: Option<f64>,
    pub bhckm717: Option<f64>,
    pub bhckm719: Option<f64>,
    pub bhckm720: Option<f64>,
    pub bhckm721: Option<f64>,
    pub bhckm722: Option<f64>,
    pub bhckm723: Option<f64>,
    pub bhckm724: Option<f64>,
    pub bhckm725: Option<f64>,
    pub bhckm726: Option<f64>,
    pub bhckm745: Option<f64>,
    pub bhckm746: Option<f64>,
    pub bhckm747: Option<f64>,
    pub bhckm748: Option<f64>,
    pub bhckm749: Option<f64>,
    pub bhckm750: Option<f64>,
    pub bhckm751: Option<f64>,
    pub bhckmg93: Option<f64>,
    pub bhckmg95: Option<f64>,
    pub bhcks413: Option<f64>,
    pub bhcks419: Option<f64>,
    pub bhcks423: Option<f64>,
    pub bhcks431: Option<f64>,
    pub bhcks439: Option<f64>,
    pub bhcks445: Option<f64>,
    pub bhcks449: Option<f64>,
    pub bhcks457: Option<f64>,
    pub bhcks466: Option<f64>,
    pub bhcks467: Option<f64>,
    pub bhcks475: Option<f64>,
    pub bhcks480: Option<f64>,
    pub bhcks485: Option<f64>,
    pub bhcks490: Option<f64>,
    pub bhcks495: Option<f64>,
    pub bhcks500: Option<f64>,
    pub bhcks503: Option<f64>,
    pub bhcks504: Option<f64>,
    pub bhcks505: Option<f64>,
    pub bhcks506: Option<f64>,
    pub bhcks507: Option<f64>,
    pub bhcks510: Option<f64>,
    pub bhcks512: Option<f64>,
    pub bhcks514: Option<f64>,
    pub bhcks515: Option<f64>,
    pub bhcks516: Option<f64>,
    pub bhcks517: Option<f64>,
    pub bhcks518: Option<f64>,
    pub bhcks519: Option<f64>,
    pub bhcks520: Option<f64>,
    pub bhcks521: Option<f64>,
    pub bhcks522: Option<f64>,
    pub bhcks523: Option<f64>,
    pub bhcks525: Option<f64>,
    pub bhcks526: Option<f64>,
    pub bhcks527: Option<f64>,
    pub bhcks528: Option<f64>,
    pub bhcks529: Option<f64>,
    pub bhcks530: Option<f64>,
    pub bhcks531: Option<f64>,
    pub bhcks539: Option<f64>,
    pub bhcks540: Option<f64>,
    pub bhcks541: Option<f64>,
    pub bhcks542: Option<f64>,
    pub bhcks543: Option<f64>,
    pub bhcks544: Option<f64>,
    pub bhcks545: Option<f64>,
    pub bhcks546: Option<f64>,
    pub bhcks547: Option<f64>,
    pub bhcks548: Option<f64>,
    pub bhcks558: Option<f64>,
    pub bhcks559: Option<f64>,
    pub bhcks560: Option<f64>,
    pub bhcks561: Option<f64>,
    pub bhcks562: Option<f64>,
    pub bhcks563: Option<f64>,
    pub bhcks564: Option<f64>,
    pub bhcks565: Option<f64>,
    pub bhcks566: Option<f64>,
    pub bhcks567: Option<f64>,
    pub bhcks568: Option<f64>,
    pub bhcks569: Option<f64>,
    pub bhcks570: Option<f64>,
    pub bhcks571: Option<f64>,
    pub bhcks572: Option<f64>,
    pub bhcks573: Option<f64>,
    pub bhcks574: Option<f64>,
    pub bhcks575: Option<f64>,
    pub bhcks576: Option<f64>,
    pub bhcks577: Option<f64>,
    pub bhcks578: Option<f64>,
    pub bhcks579: Option<f64>,
    pub bhcks580: Option<f64>,
    pub bhcks581: Option<f64>,
    pub bhcks624: Option<f64>,
    pub rssd9001: Option<f64>, // Option<i64>,
    pub rssd9017: Option<String>,
    pub rssd9999: Option<f64>, // Option<NaiveDate>,
    pub wrdsdownloaddate: Option<NaiveDate>,
}

impl SurrealCrudModel for BhckSeries2 {
    fn table() -> &'static str {
        "bhck_series2"
    }
    fn id_key(&self) -> Option<String> {
        match (self.rssd9001, self.rssd9999) {
            (Some(rssd9001), Some(rssd9999)) => Some(format!("{rssd9001}:{rssd9999}")),
            _ => None,
        }
    }
}

impl DuckCrudModel for BhckSeries2 {
    fn table() -> &'static str {
        "bhck_series2"
    }
    fn id_key(&self) -> Option<String> {
        <Self as SurrealCrudModel>::id_key(self)
    }
}

impl ToPolars for BhckSeries2 {
    fn schema() -> Schema {
        BhckSeries2::polars_schema()
    }
}

impl BhckSeries2 {
    pub fn polars_schema() -> Schema {
        Schema::from_iter(vec![
            Field::new("bhck0383".into(), DataType::Float64),
            Field::new("bhck0384".into(), DataType::Float64),
            Field::new("bhck0387".into(), DataType::Float64),
            Field::new("bhck0416".into(), DataType::Float64),
            Field::new("bhck0535".into(), DataType::Float64),
            Field::new("bhck1395".into(), DataType::Float64),
            Field::new("bhck1403".into(), DataType::Float64),
            Field::new("bhck1406".into(), DataType::Float64),
            Field::new("bhck1407".into(), DataType::Float64),
            Field::new("bhck1658".into(), DataType::Float64),
            Field::new("bhck1659".into(), DataType::Float64),
            Field::new("bhck1661".into(), DataType::Float64),
            Field::new("bhck1771".into(), DataType::Float64),
            Field::new("bhck1772".into(), DataType::Float64),
            Field::new("bhck1914".into(), DataType::Float64),
            Field::new("bhck2033".into(), DataType::Float64),
            Field::new("bhck2079".into(), DataType::Float64),
            Field::new("bhck2122".into(), DataType::Float64),
            Field::new("bhck2123".into(), DataType::Float64),
            Field::new("bhck2125".into(), DataType::Float64),
            Field::new("bhck2145".into(), DataType::Float64),
            Field::new("bhck2170".into(), DataType::Float64),
            Field::new("bhck2221".into(), DataType::Float64),
            Field::new("bhck2419".into(), DataType::Float64),
            Field::new("bhck2432".into(), DataType::Float64),
            Field::new("bhck2635".into(), DataType::Float64),
            Field::new("bhck2744".into(), DataType::Float64),
            Field::new("bhck2948".into(), DataType::Float64),
            Field::new("bhck3196".into(), DataType::Float64),
            Field::new("bhck3210".into(), DataType::Float64),
            Field::new("bhck3240".into(), DataType::Float64),
            Field::new("bhck3247".into(), DataType::Float64),
            Field::new("bhck3283".into(), DataType::Float64),
            Field::new("bhck3290".into(), DataType::Float64),
            Field::new("bhck3293".into(), DataType::Float64),
            Field::new("bhck3300".into(), DataType::Float64),
            Field::new("bhck3353".into(), DataType::Float64),
            Field::new("bhck3365".into(), DataType::Float64),
            Field::new("bhck3368".into(), DataType::Float64),
            Field::new("bhck3376".into(), DataType::Float64),
            Field::new("bhck3377".into(), DataType::Float64),
            Field::new("bhck3378".into(), DataType::Float64),
            Field::new("bhck3401".into(), DataType::Float64),
            Field::new("bhck3402".into(), DataType::Float64),
            Field::new("bhck3404".into(), DataType::Float64),
            Field::new("bhck3408".into(), DataType::Float64),
            Field::new("bhck3428".into(), DataType::Float64),
            Field::new("bhck3429".into(), DataType::Float64),
            Field::new("bhck3432".into(), DataType::Float64),
            Field::new("bhck3433".into(), DataType::Float64),
            Field::new("bhck3459".into(), DataType::Float64),
            Field::new("bhck3515".into(), DataType::Float64),
            Field::new("bhck3516".into(), DataType::Float64),
            Field::new("bhck3517".into(), DataType::Float64),
            Field::new("bhck3519".into(), DataType::Float64),
            Field::new("bhck3521".into(), DataType::Float64),
            Field::new("bhck3531".into(), DataType::Float64),
            Field::new("bhck3532".into(), DataType::Float64),
            Field::new("bhck3533".into(), DataType::Float64),
            Field::new("bhck3534".into(), DataType::Float64),
            Field::new("bhck3535".into(), DataType::Float64),
            Field::new("bhck3536".into(), DataType::Float64),
            Field::new("bhck3537".into(), DataType::Float64),
            Field::new("bhck3542".into(), DataType::Float64),
            Field::new("bhck3543".into(), DataType::Float64),
            Field::new("bhck3545".into(), DataType::Float64),
            Field::new("bhck3547".into(), DataType::Float64),
            Field::new("bhck3548".into(), DataType::Float64),
            Field::new("bhck3573".into(), DataType::Float64),
            Field::new("bhck3575".into(), DataType::Float64),
            Field::new("bhck3577".into(), DataType::Float64),
            Field::new("bhck3579".into(), DataType::Float64),
            Field::new("bhck3583".into(), DataType::Float64),
            Field::new("bhck3585".into(), DataType::Float64),
            Field::new("bhck3589".into(), DataType::Float64),
            Field::new("bhck3591".into(), DataType::Float64),
            Field::new("bhck3792".into(), DataType::Float64),
            Field::new("bhck3814".into(), DataType::Float64),
            Field::new("bhck3815".into(), DataType::Float64),
            Field::new("bhck3817".into(), DataType::Float64),
            Field::new("bhck3818".into(), DataType::Float64),
            Field::new("bhck4062".into(), DataType::Float64),
            Field::new("bhck4073".into(), DataType::Float64),
            Field::new("bhck4079".into(), DataType::Float64),
            Field::new("bhck4093".into(), DataType::Float64),
            Field::new("bhck4107".into(), DataType::Float64),
            Field::new("bhck4135".into(), DataType::Float64),
            Field::new("bhck4230".into(), DataType::Float64),
            Field::new("bhck4243".into(), DataType::Float64),
            Field::new("bhck4307".into(), DataType::Float64),
            Field::new("bhck4483".into(), DataType::Float64),
            Field::new("bhck4505".into(), DataType::Float64),
            Field::new("bhck4605".into(), DataType::Float64),
            Field::new("bhck4617".into(), DataType::Float64),
            Field::new("bhck4618".into(), DataType::Float64),
            Field::new("bhck4627".into(), DataType::Float64),
            Field::new("bhck4628".into(), DataType::Float64),
            Field::new("bhck4661".into(), DataType::Float64),
            Field::new("bhck4662".into(), DataType::Float64),
            Field::new("bhck4663".into(), DataType::Float64),
            Field::new("bhck4664".into(), DataType::Float64),
            Field::new("bhck4665".into(), DataType::Float64),
            Field::new("bhck4666".into(), DataType::Float64),
            Field::new("bhck4667".into(), DataType::Float64),
            Field::new("bhck4668".into(), DataType::Float64),
            Field::new("bhck4669".into(), DataType::Float64),
            Field::new("bhck4782".into(), DataType::Float64),
            Field::new("bhck4783".into(), DataType::Float64),
            Field::new("bhck5306".into(), DataType::Float64),
            Field::new("bhck5311".into(), DataType::Float64),
            Field::new("bhck5352".into(), DataType::Float64),
            Field::new("bhck5353".into(), DataType::Float64),
            Field::new("bhck5357".into(), DataType::Float64),
            Field::new("bhck5358".into(), DataType::Float64),
            Field::new("bhck5376".into(), DataType::Float64),
            Field::new("bhck5396".into(), DataType::Float64),
            Field::new("bhck5410".into(), DataType::Float64),
            Field::new("bhck5412".into(), DataType::Float64),
            Field::new("bhck5414".into(), DataType::Float64),
            Field::new("bhck5479".into(), DataType::Float64),
            Field::new("bhck5483".into(), DataType::Float64),
            Field::new("bhck5484".into(), DataType::Float64),
            Field::new("bhck5500".into(), DataType::Float64),
            Field::new("bhck5501".into(), DataType::Float64),
            Field::new("bhck5502".into(), DataType::Float64),
            Field::new("bhck5503".into(), DataType::Float64),
            Field::new("bhck5504".into(), DataType::Float64),
            Field::new("bhck5505".into(), DataType::Float64),
            Field::new("bhck5523".into(), DataType::Float64),
            Field::new("bhck5524".into(), DataType::Float64),
            Field::new("bhck5525".into(), DataType::Float64),
            Field::new("bhck5526".into(), DataType::Float64),
            Field::new("bhck5990".into(), DataType::Float64),
            Field::new("bhck6562".into(), DataType::Float64),
            Field::new("bhck6568".into(), DataType::Float64),
            Field::new("bhck6570".into(), DataType::Float64),
            Field::new("bhck6577".into(), DataType::Float64),
            Field::new("bhck6996".into(), DataType::Boolean),
            Field::new("bhck6997".into(), DataType::Boolean),
            Field::new("bhck7204".into(), DataType::Float64),
            Field::new("bhck7205".into(), DataType::Float64),
            Field::new("bhck7206".into(), DataType::Float64),
            Field::new("bhck8274".into(), DataType::Float64),
            Field::new("bhck8275".into(), DataType::Float64),
            Field::new("bhck8551".into(), DataType::Float64),
            Field::new("bhck8552".into(), DataType::Float64),
            Field::new("bhck8553".into(), DataType::Float64),
            Field::new("bhck8554".into(), DataType::Float64),
            Field::new("bhck8555".into(), DataType::Float64),
            Field::new("bhck8556".into(), DataType::Float64),
            Field::new("bhck8701".into(), DataType::Float64),
            Field::new("bhck8702".into(), DataType::Float64),
            Field::new("bhck8703".into(), DataType::Float64),
            Field::new("bhck8704".into(), DataType::Float64),
            Field::new("bhck8705".into(), DataType::Float64),
            Field::new("bhck8706".into(), DataType::Float64),
            Field::new("bhck8707".into(), DataType::Float64),
            Field::new("bhck8708".into(), DataType::Float64),
            Field::new("bhck8709".into(), DataType::Float64),
            Field::new("bhck8710".into(), DataType::Float64),
            Field::new("bhck8711".into(), DataType::Float64),
            Field::new("bhck8712".into(), DataType::Float64),
            Field::new("bhck8713".into(), DataType::Float64),
            Field::new("bhck8714".into(), DataType::Float64),
            Field::new("bhck8715".into(), DataType::Float64),
            Field::new("bhck8716".into(), DataType::Float64),
            Field::new("bhck8717".into(), DataType::Boolean),
            Field::new("bhck8718".into(), DataType::Boolean),
            Field::new("bhck8723".into(), DataType::Float64),
            Field::new("bhck8724".into(), DataType::Float64),
            Field::new("bhck8725".into(), DataType::Float64),
            Field::new("bhck8726".into(), DataType::Float64),
            Field::new("bhck8727".into(), DataType::Float64),
            Field::new("bhck8728".into(), DataType::Float64),
            Field::new("bhck8729".into(), DataType::Float64),
            Field::new("bhck8730".into(), DataType::Float64),
            Field::new("bhck8731".into(), DataType::Float64),
            Field::new("bhck8732".into(), DataType::Float64),
            Field::new("bhck8765".into(), DataType::Float64),
            Field::new("bhck8768".into(), DataType::Boolean),
            Field::new("bhck8784".into(), DataType::Float64),
            Field::new("bhck8834".into(), DataType::Boolean),
            Field::new("bhck8836".into(), DataType::Boolean),
            Field::new("bhck8838".into(), DataType::Boolean),
            Field::new("bhck9191".into(), DataType::Float64),
            Field::new("bhck9802".into(), DataType::Float64),
            Field::new("bhcka102".into(), DataType::Float64),
            Field::new("bhcka120".into(), DataType::Boolean),
            Field::new("bhcka121".into(), DataType::Boolean),
            Field::new("bhcka122".into(), DataType::Boolean),
            Field::new("bhcka123".into(), DataType::Boolean),
            Field::new("bhcka124".into(), DataType::Boolean),
            Field::new("bhcka126".into(), DataType::Float64),
            Field::new("bhcka127".into(), DataType::Float64),
            Field::new("bhcka128".into(), DataType::Boolean),
            Field::new("bhcka195".into(), DataType::Float64),
            Field::new("bhcka220".into(), DataType::Float64),
            Field::new("bhcka223".into(), DataType::Float64),
            Field::new("bhcka249".into(), DataType::Float64),
            Field::new("bhcka288".into(), DataType::Float64),
            Field::new("bhcka591".into(), DataType::Float64),
            Field::new("bhckb027".into(), DataType::Float64),
            Field::new("bhckb028".into(), DataType::Float64),
            Field::new("bhckb031".into(), DataType::Float64),
            Field::new("bhckb033".into(), DataType::Float64),
            Field::new("bhckb034".into(), DataType::Float64),
            Field::new("bhckb037".into(), DataType::Float64),
            Field::new("bhckb038".into(), DataType::Float64),
            Field::new("bhckb041".into(), DataType::Float64),
            Field::new("bhckb042".into(), DataType::Float64),
            Field::new("bhckb043".into(), DataType::Float64),
            Field::new("bhckb046".into(), DataType::Float64),
            Field::new("bhckb048".into(), DataType::Float64),
            Field::new("bhckb049".into(), DataType::Float64),
            Field::new("bhckb052".into(), DataType::Float64),
            Field::new("bhckb053".into(), DataType::Float64),
            Field::new("bhckb056".into(), DataType::Float64),
            Field::new("bhckb491".into(), DataType::Float64),
            Field::new("bhckb507".into(), DataType::Float64),
            Field::new("bhckb513".into(), DataType::Float64),
            Field::new("bhckb515".into(), DataType::Float64),
            Field::new("bhckb517".into(), DataType::Float64),
            Field::new("bhckb541".into(), DataType::Float64),
            Field::new("bhckb558".into(), DataType::Float64),
            Field::new("bhckb589".into(), DataType::Float64),
            Field::new("bhckb696".into(), DataType::Float64),
            Field::new("bhckb697".into(), DataType::Float64),
            Field::new("bhckb698".into(), DataType::Float64),
            Field::new("bhckb699".into(), DataType::Float64),
            Field::new("bhckb700".into(), DataType::Float64),
            Field::new("bhckb701".into(), DataType::Float64),
            Field::new("bhckb702".into(), DataType::Float64),
            Field::new("bhckb703".into(), DataType::Float64),
            Field::new("bhckb704".into(), DataType::Float64),
            Field::new("bhckb705".into(), DataType::Float64),
            Field::new("bhckb706".into(), DataType::Float64),
            Field::new("bhckb707".into(), DataType::Float64),
            Field::new("bhckb708".into(), DataType::Float64),
            Field::new("bhckb709".into(), DataType::Float64),
            Field::new("bhckb710".into(), DataType::Float64),
            Field::new("bhckb711".into(), DataType::Float64),
            Field::new("bhckb712".into(), DataType::Float64),
            Field::new("bhckb713".into(), DataType::Float64),
            Field::new("bhckb714".into(), DataType::Float64),
            Field::new("bhckb715".into(), DataType::Float64),
            Field::new("bhckb716".into(), DataType::Float64),
            Field::new("bhckb717".into(), DataType::Float64),
            Field::new("bhckb718".into(), DataType::Float64),
            Field::new("bhckb719".into(), DataType::Float64),
            Field::new("bhckb720".into(), DataType::Float64),
            Field::new("bhckb721".into(), DataType::Float64),
            Field::new("bhckb722".into(), DataType::Float64),
            Field::new("bhckb723".into(), DataType::Float64),
            Field::new("bhckb724".into(), DataType::Float64),
            Field::new("bhckb725".into(), DataType::Float64),
            Field::new("bhckb726".into(), DataType::Float64),
            Field::new("bhckb727".into(), DataType::Float64),
            Field::new("bhckb728".into(), DataType::Float64),
            Field::new("bhckb729".into(), DataType::Float64),
            Field::new("bhckb730".into(), DataType::Float64),
            Field::new("bhckb731".into(), DataType::Float64),
            Field::new("bhckb732".into(), DataType::Float64),
            Field::new("bhckb733".into(), DataType::Float64),
            Field::new("bhckb734".into(), DataType::Float64),
            Field::new("bhckb735".into(), DataType::Float64),
            Field::new("bhckb736".into(), DataType::Float64),
            Field::new("bhckb737".into(), DataType::Float64),
            Field::new("bhckb738".into(), DataType::Float64),
            Field::new("bhckb739".into(), DataType::Float64),
            Field::new("bhckb740".into(), DataType::Float64),
            Field::new("bhckb741".into(), DataType::Float64),
            Field::new("bhckb742".into(), DataType::Float64),
            Field::new("bhckb743".into(), DataType::Float64),
            Field::new("bhckb744".into(), DataType::Float64),
            Field::new("bhckb745".into(), DataType::Float64),
            Field::new("bhckb746".into(), DataType::Float64),
            Field::new("bhckb754".into(), DataType::Float64),
            Field::new("bhckb755".into(), DataType::Float64),
            Field::new("bhckb756".into(), DataType::Float64),
            Field::new("bhckb757".into(), DataType::Float64),
            Field::new("bhckb758".into(), DataType::Float64),
            Field::new("bhckb759".into(), DataType::Float64),
            Field::new("bhckb760".into(), DataType::Float64),
            Field::new("bhckb764".into(), DataType::Float64),
            Field::new("bhckb765".into(), DataType::Float64),
            Field::new("bhckb766".into(), DataType::Float64),
            Field::new("bhckb767".into(), DataType::Float64),
            Field::new("bhckb768".into(), DataType::Float64),
            Field::new("bhckb769".into(), DataType::Float64),
            Field::new("bhckb773".into(), DataType::Float64),
            Field::new("bhckb774".into(), DataType::Float64),
            Field::new("bhckb775".into(), DataType::Float64),
            Field::new("bhckb783".into(), DataType::Float64),
            Field::new("bhckb784".into(), DataType::Float64),
            Field::new("bhckb785".into(), DataType::Float64),
            Field::new("bhckb786".into(), DataType::Float64),
            Field::new("bhckb787".into(), DataType::Float64),
            Field::new("bhckb788".into(), DataType::Float64),
            Field::new("bhckb789".into(), DataType::Float64),
            Field::new("bhckb804".into(), DataType::Float64),
            Field::new("bhckb805".into(), DataType::Float64),
            Field::new("bhckb808".into(), DataType::Float64),
            Field::new("bhckb809".into(), DataType::Float64),
            Field::new("bhckb982".into(), DataType::Float64),
            Field::new("bhckb989".into(), DataType::Float64),
            Field::new("bhckb995".into(), DataType::Float64),
            Field::new("bhckb997".into(), DataType::Float64),
            Field::new("bhckc015".into(), DataType::Float64),
            Field::new("bhckc018".into(), DataType::Float64),
            Field::new("bhckc026".into(), DataType::Float64),
            Field::new("bhckc027".into(), DataType::Float64),
            Field::new("bhckc217".into(), DataType::Float64),
            Field::new("bhckc218".into(), DataType::Float64),
            Field::new("bhckc227".into(), DataType::Float64),
            Field::new("bhckc242".into(), DataType::Float64),
            Field::new("bhckc244".into(), DataType::Float64),
            Field::new("bhckc245".into(), DataType::Float64),
            Field::new("bhckc247".into(), DataType::Float64),
            Field::new("bhckc248".into(), DataType::Float64),
            Field::new("bhckc249".into(), DataType::Float64),
            Field::new("bhckc388".into(), DataType::Float64),
            Field::new("bhckc389".into(), DataType::Float64),
            Field::new("bhckc391".into(), DataType::Float64),
            Field::new("bhckc393".into(), DataType::Float64),
            Field::new("bhckc394".into(), DataType::Float64),
            Field::new("bhckc395".into(), DataType::Float64),
            Field::new("bhckc396".into(), DataType::Float64),
            Field::new("bhckc397".into(), DataType::Float64),
            Field::new("bhckc398".into(), DataType::Float64),
            Field::new("bhckc399".into(), DataType::Float64),
            Field::new("bhckc400".into(), DataType::Float64),
            Field::new("bhckc401".into(), DataType::Float64),
            Field::new("bhckc402".into(), DataType::Float64),
            Field::new("bhckc403".into(), DataType::Float64),
            Field::new("bhckc404".into(), DataType::Float64),
            Field::new("bhckc405".into(), DataType::Float64),
            Field::new("bhckc406".into(), DataType::Float64),
            Field::new("bhckc407".into(), DataType::Float64),
            Field::new("bhckc408".into(), DataType::Float64),
            Field::new("bhckc409".into(), DataType::Float64),
            Field::new("bhckc502".into(), DataType::Float64),
            Field::new("bhckc699".into(), DataType::Float64),
            Field::new("bhckc779".into(), DataType::Float64),
            Field::new("bhckc780".into(), DataType::Float64),
            Field::new("bhckc866".into(), DataType::Float64),
            Field::new("bhckc867".into(), DataType::Float64),
            Field::new("bhckc868".into(), DataType::Float64),
            Field::new("bhckd957".into(), DataType::Float64),
            Field::new("bhckd961".into(), DataType::Float64),
            Field::new("bhckd966".into(), DataType::Float64),
            Field::new("bhckd976".into(), DataType::Float64),
            Field::new("bhckd977".into(), DataType::Float64),
            Field::new("bhckd978".into(), DataType::Float64),
            Field::new("bhckd979".into(), DataType::Float64),
            Field::new("bhckd980".into(), DataType::Float64),
            Field::new("bhckd981".into(), DataType::Float64),
            Field::new("bhckd987".into(), DataType::Float64),
            Field::new("bhckd988".into(), DataType::Float64),
            Field::new("bhckd989".into(), DataType::Float64),
            Field::new("bhckd990".into(), DataType::Float64),
            Field::new("bhckd997".into(), DataType::Float64),
            Field::new("bhckd998".into(), DataType::Float64),
            Field::new("bhckd999".into(), DataType::Float64),
            Field::new("bhckf064".into(), DataType::Float64),
            Field::new("bhckf065".into(), DataType::Float64),
            Field::new("bhckf066".into(), DataType::Float64),
            Field::new("bhckf067".into(), DataType::Float64),
            Field::new("bhckf068".into(), DataType::Float64),
            Field::new("bhckf069".into(), DataType::Float64),
            Field::new("bhckf186".into(), DataType::Float64),
            Field::new("bhckf187".into(), DataType::Float64),
            Field::new("bhckf188".into(), DataType::Float64),
            Field::new("bhckf230".into(), DataType::Float64),
            Field::new("bhckf231".into(), DataType::Float64),
            Field::new("bhckf232".into(), DataType::Float64),
            Field::new("bhckf240".into(), DataType::Float64),
            Field::new("bhckf243".into(), DataType::Float64),
            Field::new("bhckf246".into(), DataType::Float64),
            Field::new("bhckf249".into(), DataType::Float64),
            Field::new("bhckf252".into(), DataType::Float64),
            Field::new("bhckf255".into(), DataType::Float64),
            Field::new("bhckf258".into(), DataType::Float64),
            Field::new("bhckf261".into(), DataType::Float64),
            Field::new("bhckf559".into(), DataType::Float64),
            Field::new("bhckf597".into(), DataType::Float64),
            Field::new("bhckf598".into(), DataType::Float64),
            Field::new("bhckf599".into(), DataType::Float64),
            Field::new("bhckf600".into(), DataType::Float64),
            Field::new("bhckf601".into(), DataType::Float64),
            Field::new("bhckf609".into(), DataType::Float64),
            Field::new("bhckf610".into(), DataType::Float64),
            Field::new("bhckf614".into(), DataType::Float64),
            Field::new("bhckf615".into(), DataType::Float64),
            Field::new("bhckf616".into(), DataType::Float64),
            Field::new("bhckf617".into(), DataType::Float64),
            Field::new("bhckf618".into(), DataType::Float64),
            Field::new("bhckf624".into(), DataType::Float64),
            Field::new("bhckf632".into(), DataType::Float64),
            Field::new("bhckf633".into(), DataType::Float64),
            Field::new("bhckf634".into(), DataType::Float64),
            Field::new("bhckf635".into(), DataType::Float64),
            Field::new("bhckf636".into(), DataType::Float64),
            Field::new("bhckf641".into(), DataType::Float64),
            Field::new("bhckf642".into(), DataType::Float64),
            Field::new("bhckf643".into(), DataType::Float64),
            Field::new("bhckf644".into(), DataType::Float64),
            Field::new("bhckf645".into(), DataType::Float64),
            Field::new("bhckf646".into(), DataType::Float64),
            Field::new("bhckf647".into(), DataType::Float64),
            Field::new("bhckf648".into(), DataType::Float64),
            Field::new("bhckf649".into(), DataType::Float64),
            Field::new("bhckf650".into(), DataType::Float64),
            Field::new("bhckf651".into(), DataType::Float64),
            Field::new("bhckf652".into(), DataType::Float64),
            Field::new("bhckf653".into(), DataType::Float64),
            Field::new("bhckf654".into(), DataType::Float64),
            Field::new("bhckf656".into(), DataType::Float64),
            Field::new("bhckf657".into(), DataType::Float64),
            Field::new("bhckf659".into(), DataType::Float64),
            Field::new("bhckf660".into(), DataType::Float64),
            Field::new("bhckf667".into(), DataType::Float64),
            Field::new("bhckf668".into(), DataType::Float64),
            Field::new("bhckf669".into(), DataType::Float64),
            Field::new("bhckf699".into(), DataType::Float64),
            Field::new("bhckf790".into(), DataType::Float64),
            Field::new("bhckf837".into(), DataType::Float64),
            Field::new("bhckf838".into(), DataType::Float64),
            Field::new("bhckf842".into(), DataType::Boolean),
            Field::new("bhckft04".into(), DataType::Float64),
            Field::new("bhckft05".into(), DataType::Float64),
            Field::new("bhckg105".into(), DataType::Float64),
            Field::new("bhckg214".into(), DataType::Float64),
            Field::new("bhckg215".into(), DataType::Float64),
            Field::new("bhckg216".into(), DataType::Float64),
            Field::new("bhckg217".into(), DataType::Float64),
            Field::new("bhckg219".into(), DataType::Float64),
            Field::new("bhckg220".into(), DataType::Float64),
            Field::new("bhckg222".into(), DataType::Float64),
            Field::new("bhckg299".into(), DataType::Float64),
            Field::new("bhckg332".into(), DataType::Float64),
            Field::new("bhckg333".into(), DataType::Float64),
            Field::new("bhckg334".into(), DataType::Float64),
            Field::new("bhckg335".into(), DataType::Float64),
            Field::new("bhckg348".into(), DataType::Float64),
            Field::new("bhckg349".into(), DataType::Float64),
            Field::new("bhckg350".into(), DataType::Float64),
            Field::new("bhckg351".into(), DataType::Float64),
            Field::new("bhckg352".into(), DataType::Float64),
            Field::new("bhckg353".into(), DataType::Float64),
            Field::new("bhckg354".into(), DataType::Float64),
            Field::new("bhckg355".into(), DataType::Float64),
            Field::new("bhckg356".into(), DataType::Float64),
            Field::new("bhckg357".into(), DataType::Float64),
            Field::new("bhckg358".into(), DataType::Float64),
            Field::new("bhckg359".into(), DataType::Float64),
            Field::new("bhckg360".into(), DataType::Float64),
            Field::new("bhckg361".into(), DataType::Float64),
            Field::new("bhckg362".into(), DataType::Float64),
            Field::new("bhckg363".into(), DataType::Float64),
            Field::new("bhckg364".into(), DataType::Float64),
            Field::new("bhckg365".into(), DataType::Float64),
            Field::new("bhckg366".into(), DataType::Float64),
            Field::new("bhckg367".into(), DataType::Float64),
            Field::new("bhckg368".into(), DataType::Float64),
            Field::new("bhckg369".into(), DataType::Float64),
            Field::new("bhckg370".into(), DataType::Float64),
            Field::new("bhckg371".into(), DataType::Float64),
            Field::new("bhckg372".into(), DataType::Float64),
            Field::new("bhckg373".into(), DataType::Float64),
            Field::new("bhckg374".into(), DataType::Float64),
            Field::new("bhckg375".into(), DataType::Float64),
            Field::new("bhckg378".into(), DataType::Float64),
            Field::new("bhckg379".into(), DataType::Float64),
            Field::new("bhckg380".into(), DataType::Float64),
            Field::new("bhckg381".into(), DataType::Float64),
            Field::new("bhckg382".into(), DataType::Float64),
            Field::new("bhckg383".into(), DataType::Float64),
            Field::new("bhckg384".into(), DataType::Float64),
            Field::new("bhckg385".into(), DataType::Float64),
            Field::new("bhckg386".into(), DataType::Float64),
            Field::new("bhckg387".into(), DataType::Float64),
            Field::new("bhckg388".into(), DataType::Float64),
            Field::new("bhckg418".into(), DataType::Float64),
            Field::new("bhckg419".into(), DataType::Float64),
            Field::new("bhckg420".into(), DataType::Float64),
            Field::new("bhckg421".into(), DataType::Float64),
            Field::new("bhckg422".into(), DataType::Float64),
            Field::new("bhckg423".into(), DataType::Float64),
            Field::new("bhckg424".into(), DataType::Float64),
            Field::new("bhckg425".into(), DataType::Float64),
            Field::new("bhckg426".into(), DataType::Float64),
            Field::new("bhckg427".into(), DataType::Float64),
            Field::new("bhckg428".into(), DataType::Float64),
            Field::new("bhckg429".into(), DataType::Float64),
            Field::new("bhckg430".into(), DataType::Float64),
            Field::new("bhckg431".into(), DataType::Float64),
            Field::new("bhckg432".into(), DataType::Float64),
            Field::new("bhckg433".into(), DataType::Float64),
            Field::new("bhckg434".into(), DataType::Float64),
            Field::new("bhckg435".into(), DataType::Float64),
            Field::new("bhckg436".into(), DataType::Float64),
            Field::new("bhckg437".into(), DataType::Float64),
            Field::new("bhckg438".into(), DataType::Float64),
            Field::new("bhckg439".into(), DataType::Float64),
            Field::new("bhckg440".into(), DataType::Float64),
            Field::new("bhckg441".into(), DataType::Float64),
            Field::new("bhckg442".into(), DataType::Float64),
            Field::new("bhckg443".into(), DataType::Float64),
            Field::new("bhckg444".into(), DataType::Float64),
            Field::new("bhckg445".into(), DataType::Float64),
            Field::new("bhckg446".into(), DataType::Float64),
            Field::new("bhckg447".into(), DataType::Float64),
            Field::new("bhckg448".into(), DataType::Float64),
            Field::new("bhckg449".into(), DataType::Float64),
            Field::new("bhckg450".into(), DataType::Float64),
            Field::new("bhckg451".into(), DataType::Float64),
            Field::new("bhckg452".into(), DataType::Float64),
            Field::new("bhckg453".into(), DataType::Float64),
            Field::new("bhckg454".into(), DataType::Float64),
            Field::new("bhckg455".into(), DataType::Float64),
            Field::new("bhckg456".into(), DataType::Float64),
            Field::new("bhckg457".into(), DataType::Float64),
            Field::new("bhckg458".into(), DataType::Float64),
            Field::new("bhckg459".into(), DataType::Float64),
            Field::new("bhckg460".into(), DataType::Float64),
            Field::new("bhckg461".into(), DataType::Float64),
            Field::new("bhckg462".into(), DataType::Float64),
            Field::new("bhckg493".into(), DataType::Float64),
            Field::new("bhckg494".into(), DataType::Float64),
            Field::new("bhckg495".into(), DataType::Float64),
            Field::new("bhckg496".into(), DataType::Float64),
            Field::new("bhckg497".into(), DataType::Float64),
            Field::new("bhckg498".into(), DataType::Float64),
            Field::new("bhckg499".into(), DataType::Float64),
            Field::new("bhckg500".into(), DataType::Float64),
            Field::new("bhckg501".into(), DataType::Float64),
            Field::new("bhckg502".into(), DataType::Float64),
            Field::new("bhckg503".into(), DataType::Float64),
            Field::new("bhckg504".into(), DataType::Float64),
            Field::new("bhckg505".into(), DataType::Float64),
            Field::new("bhckg506".into(), DataType::Float64),
            Field::new("bhckg512".into(), DataType::Float64),
            Field::new("bhckg513".into(), DataType::Float64),
            Field::new("bhckg514".into(), DataType::Float64),
            Field::new("bhckg515".into(), DataType::Float64),
            Field::new("bhckg516".into(), DataType::Float64),
            Field::new("bhckg517".into(), DataType::Float64),
            Field::new("bhckg518".into(), DataType::Float64),
            Field::new("bhckg519".into(), DataType::Float64),
            Field::new("bhckg520".into(), DataType::Float64),
            Field::new("bhckg526".into(), DataType::Float64),
            Field::new("bhckg527".into(), DataType::Float64),
            Field::new("bhckg528".into(), DataType::Float64),
            Field::new("bhckg529".into(), DataType::Float64),
            Field::new("bhckg530".into(), DataType::Float64),
            Field::new("bhckg531".into(), DataType::Float64),
            Field::new("bhckg532".into(), DataType::Float64),
            Field::new("bhckg533".into(), DataType::Float64),
            Field::new("bhckg534".into(), DataType::Float64),
            Field::new("bhckg535".into(), DataType::Float64),
            Field::new("bhckg551".into(), DataType::Float64),
            Field::new("bhckg552".into(), DataType::Float64),
            Field::new("bhckg553".into(), DataType::Float64),
            Field::new("bhckg554".into(), DataType::Float64),
            Field::new("bhckg555".into(), DataType::Float64),
            Field::new("bhckg556".into(), DataType::Float64),
            Field::new("bhckg557".into(), DataType::Float64),
            Field::new("bhckg558".into(), DataType::Float64),
            Field::new("bhckg559".into(), DataType::Float64),
            Field::new("bhckg560".into(), DataType::Float64),
            Field::new("bhckg576".into(), DataType::Float64),
            Field::new("bhckg577".into(), DataType::Float64),
            Field::new("bhckg578".into(), DataType::Float64),
            Field::new("bhckg579".into(), DataType::Float64),
            Field::new("bhckg580".into(), DataType::Float64),
            Field::new("bhckg581".into(), DataType::Float64),
            Field::new("bhckg582".into(), DataType::Float64),
            Field::new("bhckg583".into(), DataType::Float64),
            Field::new("bhckg584".into(), DataType::Float64),
            Field::new("bhckg585".into(), DataType::Float64),
            Field::new("bhckg591".into(), DataType::Float64),
            Field::new("bhckg603".into(), DataType::Float64),
            Field::new("bhckg604".into(), DataType::Float64),
            Field::new("bhckg605".into(), DataType::Float64),
            Field::new("bhckg612".into(), DataType::Float64),
            Field::new("bhckg613".into(), DataType::Float64),
            Field::new("bhckg614".into(), DataType::Float64),
            Field::new("bhckg615".into(), DataType::Float64),
            Field::new("bhckg616".into(), DataType::Float64),
            Field::new("bhckg617".into(), DataType::Float64),
            Field::new("bhckg624".into(), DataType::Float64),
            Field::new("bhckg625".into(), DataType::Float64),
            Field::new("bhckg626".into(), DataType::Float64),
            Field::new("bhckg627".into(), DataType::Float64),
            Field::new("bhckg628".into(), DataType::Float64),
            Field::new("bhckg629".into(), DataType::Float64),
            Field::new("bhckg630".into(), DataType::Float64),
            Field::new("bhckg631".into(), DataType::Float64),
            Field::new("bhckg632".into(), DataType::Float64),
            Field::new("bhckg633".into(), DataType::Float64),
            Field::new("bhckg634".into(), DataType::Float64),
            Field::new("bhckg635".into(), DataType::Float64),
            Field::new("bhckg636".into(), DataType::Float64),
            Field::new("bhckg637".into(), DataType::Float64),
            Field::new("bhckg641".into(), DataType::Float64),
            Field::new("bhckg651".into(), DataType::Float64),
            Field::new("bhckg652".into(), DataType::Float64),
            Field::new("bhckh171".into(), DataType::Float64),
            Field::new("bhckh191".into(), DataType::Float64),
            Field::new("bhckh289".into(), DataType::Float64),
            Field::new("bhckh290".into(), DataType::Float64),
            Field::new("bhckh291".into(), DataType::Float64),
            Field::new("bhckh292".into(), DataType::Float64),
            Field::new("bhckh300".into(), DataType::Float64),
            Field::new("bhckh301".into(), DataType::Float64),
            Field::new("bhckh302".into(), DataType::Float64),
            Field::new("bhckh303".into(), DataType::Float64),
            Field::new("bhckh304".into(), DataType::Float64),
            Field::new("bhckh307".into(), DataType::Float64),
            Field::new("bhckh308".into(), DataType::Float64),
            Field::new("bhckh309".into(), DataType::Float64),
            Field::new("bhckh310".into(), DataType::Float64),
            Field::new("bhckhj74".into(), DataType::Float64),
            Field::new("bhckhj75".into(), DataType::Float64),
            Field::new("bhckhj76".into(), DataType::Float64),
            Field::new("bhckhj77".into(), DataType::Float64),
            Field::new("bhckhj86".into(), DataType::Float64),
            Field::new("bhckhj87".into(), DataType::Float64),
            Field::new("bhckhj90".into(), DataType::Float64),
            Field::new("bhckhj91".into(), DataType::Float64),
            Field::new("bhckhj96".into(), DataType::Float64),
            Field::new("bhckhj97".into(), DataType::Float64),
            Field::new("bhckhj98".into(), DataType::Float64),
            Field::new("bhckhj99".into(), DataType::Float64),
            Field::new("bhckhk00".into(), DataType::Float64),
            Field::new("bhckhk01".into(), DataType::Float64),
            Field::new("bhckhk25".into(), DataType::Float64),
            Field::new("bhckhk26".into(), DataType::Float64),
            Field::new("bhckhk27".into(), DataType::Float64),
            Field::new("bhckhk28".into(), DataType::Float64),
            Field::new("bhckht50".into(), DataType::Float64),
            Field::new("bhckht51".into(), DataType::Float64),
            Field::new("bhckht52".into(), DataType::Float64),
            Field::new("bhckht53".into(), DataType::Float64),
            Field::new("bhckht66".into(), DataType::Float64),
            Field::new("bhckht67".into(), DataType::Float64),
            Field::new("bhckht68".into(), DataType::Float64),
            Field::new("bhckht70".into(), DataType::Float64),
            Field::new("bhckht81".into(), DataType::Float64),
            Field::new("bhckht82".into(), DataType::Float64),
            Field::new("bhckht86".into(), DataType::Float64),
            Field::new("bhckhu16".into(), DataType::Float64),
            Field::new("bhckhu17".into(), DataType::Float64),
            Field::new("bhckhu18".into(), DataType::Float64),
            Field::new("bhckj319".into(), DataType::Float64),
            Field::new("bhckj321".into(), DataType::Float64),
            Field::new("bhckj457".into(), DataType::Float64),
            Field::new("bhckj458".into(), DataType::Float64),
            Field::new("bhckj459".into(), DataType::Float64),
            Field::new("bhckjf77".into(), DataType::Float64),
            Field::new("bhckjf78".into(), DataType::Float64),
            Field::new("bhckjh89".into(), DataType::Float64),
            Field::new("bhckjh90".into(), DataType::Float64),
            Field::new("bhckjh95".into(), DataType::Float64),
            Field::new("bhckjh96".into(), DataType::Float64),
            Field::new("bhckjj02".into(), DataType::Float64),
            Field::new("bhckjj33".into(), DataType::Float64),
            Field::new("bhckk042".into(), DataType::Float64),
            Field::new("bhckk043".into(), DataType::Float64),
            Field::new("bhckk044".into(), DataType::Float64),
            Field::new("bhckk102".into(), DataType::Float64),
            Field::new("bhckk103".into(), DataType::Float64),
            Field::new("bhckk104".into(), DataType::Float64),
            Field::new("bhckk133".into(), DataType::Float64),
            Field::new("bhckk141".into(), DataType::Float64),
            Field::new("bhckk195".into(), DataType::Float64),
            Field::new("bhckk197".into(), DataType::Float64),
            Field::new("bhckk198".into(), DataType::Float64),
            Field::new("bhckk199".into(), DataType::Float64),
            Field::new("bhckk200".into(), DataType::Float64),
            Field::new("bhckk206".into(), DataType::Float64),
            Field::new("bhckk209".into(), DataType::Float64),
            Field::new("bhckk210".into(), DataType::Float64),
            Field::new("bhckk211".into(), DataType::Float64),
            Field::new("bhckkx48".into(), DataType::Float64),
            Field::new("bhckkx49".into(), DataType::Float64),
            Field::new("bhckkx56".into(), DataType::Float64),
            Field::new("bhckkx59".into(), DataType::Float64),
            Field::new("bhckkx66".into(), DataType::Float64),
            Field::new("bhckkx67".into(), DataType::Float64),
            Field::new("bhckkx68".into(), DataType::Float64),
            Field::new("bhckl183".into(), DataType::Float64),
            Field::new("bhckl184".into(), DataType::Float64),
            Field::new("bhckl185".into(), DataType::Float64),
            Field::new("bhckl186".into(), DataType::Float64),
            Field::new("bhckl187".into(), DataType::Float64),
            Field::new("bhckl188".into(), DataType::Float64),
            Field::new("bhckl191".into(), DataType::Boolean),
            Field::new("bhckl192".into(), DataType::Boolean),
            Field::new("bhckle75".into(), DataType::Float64),
            Field::new("bhcklg25".into(), DataType::Boolean),
            Field::new("bhcklg27".into(), DataType::Float64),
            Field::new("bhcklg28".into(), DataType::Float64),
            Field::new("bhckll57".into(), DataType::Float64),
            Field::new("bhckm288".into(), DataType::Float64),
            Field::new("bhckm708".into(), DataType::Float64),
            Field::new("bhckm709".into(), DataType::Float64),
            Field::new("bhckm710".into(), DataType::Float64),
            Field::new("bhckm711".into(), DataType::Float64),
            Field::new("bhckm712".into(), DataType::Float64),
            Field::new("bhckm713".into(), DataType::Float64),
            Field::new("bhckm714".into(), DataType::Float64),
            Field::new("bhckm715".into(), DataType::Float64),
            Field::new("bhckm716".into(), DataType::Float64),
            Field::new("bhckm717".into(), DataType::Float64),
            Field::new("bhckm719".into(), DataType::Float64),
            Field::new("bhckm720".into(), DataType::Float64),
            Field::new("bhckm721".into(), DataType::Float64),
            Field::new("bhckm722".into(), DataType::Float64),
            Field::new("bhckm723".into(), DataType::Float64),
            Field::new("bhckm724".into(), DataType::Float64),
            Field::new("bhckm725".into(), DataType::Float64),
            Field::new("bhckm726".into(), DataType::Float64),
            Field::new("bhckm745".into(), DataType::Float64),
            Field::new("bhckm746".into(), DataType::Float64),
            Field::new("bhckm747".into(), DataType::Float64),
            Field::new("bhckm748".into(), DataType::Float64),
            Field::new("bhckm749".into(), DataType::Float64),
            Field::new("bhckm750".into(), DataType::Float64),
            Field::new("bhckm751".into(), DataType::Float64),
            Field::new("bhckmg93".into(), DataType::Float64),
            Field::new("bhckmg95".into(), DataType::Float64),
            Field::new("bhcks413".into(), DataType::Float64),
            Field::new("bhcks419".into(), DataType::Float64),
            Field::new("bhcks423".into(), DataType::Float64),
            Field::new("bhcks431".into(), DataType::Float64),
            Field::new("bhcks439".into(), DataType::Float64),
            Field::new("bhcks445".into(), DataType::Float64),
            Field::new("bhcks449".into(), DataType::Float64),
            Field::new("bhcks457".into(), DataType::Float64),
            Field::new("bhcks466".into(), DataType::Float64),
            Field::new("bhcks467".into(), DataType::Float64),
            Field::new("bhcks475".into(), DataType::Float64),
            Field::new("bhcks480".into(), DataType::Float64),
            Field::new("bhcks485".into(), DataType::Float64),
            Field::new("bhcks490".into(), DataType::Float64),
            Field::new("bhcks495".into(), DataType::Float64),
            Field::new("bhcks500".into(), DataType::Float64),
            Field::new("bhcks503".into(), DataType::Float64),
            Field::new("bhcks504".into(), DataType::Float64),
            Field::new("bhcks505".into(), DataType::Float64),
            Field::new("bhcks506".into(), DataType::Float64),
            Field::new("bhcks507".into(), DataType::Float64),
            Field::new("bhcks510".into(), DataType::Float64),
            Field::new("bhcks512".into(), DataType::Float64),
            Field::new("bhcks514".into(), DataType::Float64),
            Field::new("bhcks515".into(), DataType::Float64),
            Field::new("bhcks516".into(), DataType::Float64),
            Field::new("bhcks517".into(), DataType::Float64),
            Field::new("bhcks518".into(), DataType::Float64),
            Field::new("bhcks519".into(), DataType::Float64),
            Field::new("bhcks520".into(), DataType::Float64),
            Field::new("bhcks521".into(), DataType::Float64),
            Field::new("bhcks522".into(), DataType::Float64),
            Field::new("bhcks523".into(), DataType::Float64),
            Field::new("bhcks525".into(), DataType::Float64),
            Field::new("bhcks526".into(), DataType::Float64),
            Field::new("bhcks527".into(), DataType::Float64),
            Field::new("bhcks528".into(), DataType::Float64),
            Field::new("bhcks529".into(), DataType::Float64),
            Field::new("bhcks530".into(), DataType::Float64),
            Field::new("bhcks531".into(), DataType::Float64),
            Field::new("bhcks539".into(), DataType::Float64),
            Field::new("bhcks540".into(), DataType::Float64),
            Field::new("bhcks541".into(), DataType::Float64),
            Field::new("bhcks542".into(), DataType::Float64),
            Field::new("bhcks543".into(), DataType::Float64),
            Field::new("bhcks544".into(), DataType::Float64),
            Field::new("bhcks545".into(), DataType::Float64),
            Field::new("bhcks546".into(), DataType::Float64),
            Field::new("bhcks547".into(), DataType::Float64),
            Field::new("bhcks548".into(), DataType::Float64),
            Field::new("bhcks558".into(), DataType::Float64),
            Field::new("bhcks559".into(), DataType::Float64),
            Field::new("bhcks560".into(), DataType::Float64),
            Field::new("bhcks561".into(), DataType::Float64),
            Field::new("bhcks562".into(), DataType::Float64),
            Field::new("bhcks563".into(), DataType::Float64),
            Field::new("bhcks564".into(), DataType::Float64),
            Field::new("bhcks565".into(), DataType::Float64),
            Field::new("bhcks566".into(), DataType::Float64),
            Field::new("bhcks567".into(), DataType::Float64),
            Field::new("bhcks568".into(), DataType::Float64),
            Field::new("bhcks569".into(), DataType::Float64),
            Field::new("bhcks570".into(), DataType::Float64),
            Field::new("bhcks571".into(), DataType::Float64),
            Field::new("bhcks572".into(), DataType::Float64),
            Field::new("bhcks573".into(), DataType::Float64),
            Field::new("bhcks574".into(), DataType::Float64),
            Field::new("bhcks575".into(), DataType::Float64),
            Field::new("bhcks576".into(), DataType::Float64),
            Field::new("bhcks577".into(), DataType::Float64),
            Field::new("bhcks578".into(), DataType::Float64),
            Field::new("bhcks579".into(), DataType::Float64),
            Field::new("bhcks580".into(), DataType::Float64),
            Field::new("bhcks581".into(), DataType::Float64),
            Field::new("bhcks624".into(), DataType::Float64),
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
    CAST(bhck0383 AS DOUBLE) AS bhck0383,
    CAST(bhck0384 AS DOUBLE) AS bhck0384,
    CAST(bhck0387 AS DOUBLE) AS bhck0387,
    CAST(bhck0416 AS DOUBLE) AS bhck0416,
    CAST(bhck0535 AS DOUBLE) AS bhck0535,
    CAST(bhck1395 AS DOUBLE) AS bhck1395,
    CAST(bhck1403 AS DOUBLE) AS bhck1403,
    CAST(bhck1406 AS DOUBLE) AS bhck1406,
    CAST(bhck1407 AS DOUBLE) AS bhck1407,
    CAST(bhck1658 AS DOUBLE) AS bhck1658,
    CAST(bhck1659 AS DOUBLE) AS bhck1659,
    CAST(bhck1661 AS DOUBLE) AS bhck1661,
    CAST(bhck1771 AS DOUBLE) AS bhck1771,
    CAST(bhck1772 AS DOUBLE) AS bhck1772,
    CAST(bhck1914 AS DOUBLE) AS bhck1914,
    CAST(bhck2033 AS DOUBLE) AS bhck2033,
    CAST(bhck2079 AS DOUBLE) AS bhck2079,
    CAST(bhck2122 AS DOUBLE) AS bhck2122,
    CAST(bhck2123 AS DOUBLE) AS bhck2123,
    CAST(bhck2125 AS DOUBLE) AS bhck2125,
    CAST(bhck2145 AS DOUBLE) AS bhck2145,
    CAST(bhck2170 AS DOUBLE) AS bhck2170,
    CAST(bhck2221 AS DOUBLE) AS bhck2221,
    CAST(bhck2419 AS DOUBLE) AS bhck2419,
    CAST(bhck2432 AS DOUBLE) AS bhck2432,
    CAST(bhck2635 AS DOUBLE) AS bhck2635,
    CAST(bhck2744 AS DOUBLE) AS bhck2744,
    CAST(bhck2948 AS DOUBLE) AS bhck2948,
    CAST(bhck3196 AS DOUBLE) AS bhck3196,
    CAST(bhck3210 AS DOUBLE) AS bhck3210,
    CAST(bhck3240 AS DOUBLE) AS bhck3240,
    CAST(bhck3247 AS DOUBLE) AS bhck3247,
    CAST(bhck3283 AS DOUBLE) AS bhck3283,
    CAST(bhck3290 AS DOUBLE) AS bhck3290,
    CAST(bhck3293 AS DOUBLE) AS bhck3293,
    CAST(bhck3300 AS DOUBLE) AS bhck3300,
    CAST(bhck3353 AS DOUBLE) AS bhck3353,
    CAST(bhck3365 AS DOUBLE) AS bhck3365,
    CAST(bhck3368 AS DOUBLE) AS bhck3368,
    CAST(bhck3376 AS DOUBLE) AS bhck3376,
    CAST(bhck3377 AS DOUBLE) AS bhck3377,
    CAST(bhck3378 AS DOUBLE) AS bhck3378,
    CAST(bhck3401 AS DOUBLE) AS bhck3401,
    CAST(bhck3402 AS DOUBLE) AS bhck3402,
    CAST(bhck3404 AS DOUBLE) AS bhck3404,
    CAST(bhck3408 AS DOUBLE) AS bhck3408,
    CAST(bhck3428 AS DOUBLE) AS bhck3428,
    CAST(bhck3429 AS DOUBLE) AS bhck3429,
    CAST(bhck3432 AS DOUBLE) AS bhck3432,
    CAST(bhck3433 AS DOUBLE) AS bhck3433,
    CAST(bhck3459 AS DOUBLE) AS bhck3459,
    CAST(bhck3515 AS DOUBLE) AS bhck3515,
    CAST(bhck3516 AS DOUBLE) AS bhck3516,
    CAST(bhck3517 AS DOUBLE) AS bhck3517,
    CAST(bhck3519 AS DOUBLE) AS bhck3519,
    CAST(bhck3521 AS DOUBLE) AS bhck3521,
    CAST(bhck3531 AS DOUBLE) AS bhck3531,
    CAST(bhck3532 AS DOUBLE) AS bhck3532,
    CAST(bhck3533 AS DOUBLE) AS bhck3533,
    CAST(bhck3534 AS DOUBLE) AS bhck3534,
    CAST(bhck3535 AS DOUBLE) AS bhck3535,
    CAST(bhck3536 AS DOUBLE) AS bhck3536,
    CAST(bhck3537 AS DOUBLE) AS bhck3537,
    CAST(bhck3542 AS DOUBLE) AS bhck3542,
    CAST(bhck3543 AS DOUBLE) AS bhck3543,
    CAST(bhck3545 AS DOUBLE) AS bhck3545,
    CAST(bhck3547 AS DOUBLE) AS bhck3547,
    CAST(bhck3548 AS DOUBLE) AS bhck3548,
    CAST(bhck3573 AS DOUBLE) AS bhck3573,
    CAST(bhck3575 AS DOUBLE) AS bhck3575,
    CAST(bhck3577 AS DOUBLE) AS bhck3577,
    CAST(bhck3579 AS DOUBLE) AS bhck3579,
    CAST(bhck3583 AS DOUBLE) AS bhck3583,
    CAST(bhck3585 AS DOUBLE) AS bhck3585,
    CAST(bhck3589 AS DOUBLE) AS bhck3589,
    CAST(bhck3591 AS DOUBLE) AS bhck3591,
    CAST(bhck3792 AS DOUBLE) AS bhck3792,
    CAST(bhck3814 AS DOUBLE) AS bhck3814,
    CAST(bhck3815 AS DOUBLE) AS bhck3815,
    CAST(bhck3817 AS DOUBLE) AS bhck3817,
    CAST(bhck3818 AS DOUBLE) AS bhck3818,
    CAST(bhck4062 AS DOUBLE) AS bhck4062,
    CAST(bhck4073 AS DOUBLE) AS bhck4073,
    CAST(bhck4079 AS DOUBLE) AS bhck4079,
    CAST(bhck4093 AS DOUBLE) AS bhck4093,
    CAST(bhck4107 AS DOUBLE) AS bhck4107,
    CAST(bhck4135 AS DOUBLE) AS bhck4135,
    CAST(bhck4230 AS DOUBLE) AS bhck4230,
    CAST(bhck4243 AS DOUBLE) AS bhck4243,
    CAST(bhck4307 AS DOUBLE) AS bhck4307,
    CAST(bhck4483 AS DOUBLE) AS bhck4483,
    CAST(bhck4505 AS DOUBLE) AS bhck4505,
    CAST(bhck4605 AS DOUBLE) AS bhck4605,
    CAST(bhck4617 AS DOUBLE) AS bhck4617,
    CAST(bhck4618 AS DOUBLE) AS bhck4618,
    CAST(bhck4627 AS DOUBLE) AS bhck4627,
    CAST(bhck4628 AS DOUBLE) AS bhck4628,
    CAST(bhck4661 AS DOUBLE) AS bhck4661,
    CAST(bhck4662 AS DOUBLE) AS bhck4662,
    CAST(bhck4663 AS DOUBLE) AS bhck4663,
    CAST(bhck4664 AS DOUBLE) AS bhck4664,
    CAST(bhck4665 AS DOUBLE) AS bhck4665,
    CAST(bhck4666 AS DOUBLE) AS bhck4666,
    CAST(bhck4667 AS DOUBLE) AS bhck4667,
    CAST(bhck4668 AS DOUBLE) AS bhck4668,
    CAST(bhck4669 AS DOUBLE) AS bhck4669,
    CAST(bhck4782 AS DOUBLE) AS bhck4782,
    CAST(bhck4783 AS DOUBLE) AS bhck4783,
    CAST(bhck5306 AS DOUBLE) AS bhck5306,
    CAST(bhck5311 AS DOUBLE) AS bhck5311,
    CAST(bhck5352 AS DOUBLE) AS bhck5352,
    CAST(bhck5353 AS DOUBLE) AS bhck5353,
    CAST(bhck5357 AS DOUBLE) AS bhck5357,
    CAST(bhck5358 AS DOUBLE) AS bhck5358,
    CAST(bhck5376 AS DOUBLE) AS bhck5376,
    CAST(bhck5396 AS DOUBLE) AS bhck5396,
    CAST(bhck5410 AS DOUBLE) AS bhck5410,
    CAST(bhck5412 AS DOUBLE) AS bhck5412,
    CAST(bhck5414 AS DOUBLE) AS bhck5414,
    CAST(bhck5479 AS DOUBLE) AS bhck5479,
    CAST(bhck5483 AS DOUBLE) AS bhck5483,
    CAST(bhck5484 AS DOUBLE) AS bhck5484,
    CAST(bhck5500 AS DOUBLE) AS bhck5500,
    CAST(bhck5501 AS DOUBLE) AS bhck5501,
    CAST(bhck5502 AS DOUBLE) AS bhck5502,
    CAST(bhck5503 AS DOUBLE) AS bhck5503,
    CAST(bhck5504 AS DOUBLE) AS bhck5504,
    CAST(bhck5505 AS DOUBLE) AS bhck5505,
    CAST(bhck5523 AS DOUBLE) AS bhck5523,
    CAST(bhck5524 AS DOUBLE) AS bhck5524,
    CAST(bhck5525 AS DOUBLE) AS bhck5525,
    CAST(bhck5526 AS DOUBLE) AS bhck5526,
    CAST(bhck5990 AS DOUBLE) AS bhck5990,
    CAST(bhck6562 AS DOUBLE) AS bhck6562,
    CAST(bhck6568 AS DOUBLE) AS bhck6568,
    CAST(bhck6570 AS DOUBLE) AS bhck6570,
    CAST(bhck6577 AS DOUBLE) AS bhck6577,
    CAST(bhck6996 AS BOOLEAN) AS bhck6996,
    CAST(bhck6997 AS BOOLEAN) AS bhck6997,
    CAST(bhck7204 AS DOUBLE) AS bhck7204,
    CAST(bhck7205 AS DOUBLE) AS bhck7205,
    CAST(bhck7206 AS DOUBLE) AS bhck7206,
    CAST(bhck8274 AS DOUBLE) AS bhck8274,
    CAST(bhck8275 AS DOUBLE) AS bhck8275,
    CAST(bhck8551 AS DOUBLE) AS bhck8551,
    CAST(bhck8552 AS DOUBLE) AS bhck8552,
    CAST(bhck8553 AS DOUBLE) AS bhck8553,
    CAST(bhck8554 AS DOUBLE) AS bhck8554,
    CAST(bhck8555 AS DOUBLE) AS bhck8555,
    CAST(bhck8556 AS DOUBLE) AS bhck8556,
    CAST(bhck8701 AS DOUBLE) AS bhck8701,
    CAST(bhck8702 AS DOUBLE) AS bhck8702,
    CAST(bhck8703 AS DOUBLE) AS bhck8703,
    CAST(bhck8704 AS DOUBLE) AS bhck8704,
    CAST(bhck8705 AS DOUBLE) AS bhck8705,
    CAST(bhck8706 AS DOUBLE) AS bhck8706,
    CAST(bhck8707 AS DOUBLE) AS bhck8707,
    CAST(bhck8708 AS DOUBLE) AS bhck8708,
    CAST(bhck8709 AS DOUBLE) AS bhck8709,
    CAST(bhck8710 AS DOUBLE) AS bhck8710,
    CAST(bhck8711 AS DOUBLE) AS bhck8711,
    CAST(bhck8712 AS DOUBLE) AS bhck8712,
    CAST(bhck8713 AS DOUBLE) AS bhck8713,
    CAST(bhck8714 AS DOUBLE) AS bhck8714,
    CAST(bhck8715 AS DOUBLE) AS bhck8715,
    CAST(bhck8716 AS DOUBLE) AS bhck8716,
    CAST(bhck8717 AS BOOLEAN) AS bhck8717,
    CAST(bhck8718 AS BOOLEAN) AS bhck8718,
    CAST(bhck8723 AS DOUBLE) AS bhck8723,
    CAST(bhck8724 AS DOUBLE) AS bhck8724,
    CAST(bhck8725 AS DOUBLE) AS bhck8725,
    CAST(bhck8726 AS DOUBLE) AS bhck8726,
    CAST(bhck8727 AS DOUBLE) AS bhck8727,
    CAST(bhck8728 AS DOUBLE) AS bhck8728,
    CAST(bhck8729 AS DOUBLE) AS bhck8729,
    CAST(bhck8730 AS DOUBLE) AS bhck8730,
    CAST(bhck8731 AS DOUBLE) AS bhck8731,
    CAST(bhck8732 AS DOUBLE) AS bhck8732,
    CAST(bhck8765 AS DOUBLE) AS bhck8765,
    CAST(bhck8768 AS BOOLEAN) AS bhck8768,
    CAST(bhck8784 AS DOUBLE) AS bhck8784,
    CAST(bhck8834 AS BOOLEAN) AS bhck8834,
    CAST(bhck8836 AS BOOLEAN) AS bhck8836,
    CAST(bhck8838 AS BOOLEAN) AS bhck8838,
    CAST(bhck9191 AS DOUBLE) AS bhck9191,
    CAST(bhck9802 AS DOUBLE) AS bhck9802,
    CAST(bhcka102 AS DOUBLE) AS bhcka102,
    CAST(bhcka120 AS BOOLEAN) AS bhcka120,
    CAST(bhcka121 AS BOOLEAN) AS bhcka121,
    CAST(bhcka122 AS BOOLEAN) AS bhcka122,
    CAST(bhcka123 AS BOOLEAN) AS bhcka123,
    CAST(bhcka124 AS BOOLEAN) AS bhcka124,
    CAST(bhcka126 AS DOUBLE) AS bhcka126,
    CAST(bhcka127 AS DOUBLE) AS bhcka127,
    CAST(bhcka128 AS BOOLEAN) AS bhcka128,
    CAST(bhcka195 AS DOUBLE) AS bhcka195,
    CAST(bhcka220 AS DOUBLE) AS bhcka220,
    CAST(bhcka223 AS DOUBLE) AS bhcka223,
    CAST(bhcka249 AS DOUBLE) AS bhcka249,
    CAST(bhcka288 AS DOUBLE) AS bhcka288,
    CAST(bhcka591 AS DOUBLE) AS bhcka591,
    CAST(bhckb027 AS DOUBLE) AS bhckb027,
    CAST(bhckb028 AS DOUBLE) AS bhckb028,
    CAST(bhckb031 AS DOUBLE) AS bhckb031,
    CAST(bhckb033 AS DOUBLE) AS bhckb033,
    CAST(bhckb034 AS DOUBLE) AS bhckb034,
    CAST(bhckb037 AS DOUBLE) AS bhckb037,
    CAST(bhckb038 AS DOUBLE) AS bhckb038,
    CAST(bhckb041 AS DOUBLE) AS bhckb041,
    CAST(bhckb042 AS DOUBLE) AS bhckb042,
    CAST(bhckb043 AS DOUBLE) AS bhckb043,
    CAST(bhckb046 AS DOUBLE) AS bhckb046,
    CAST(bhckb048 AS DOUBLE) AS bhckb048,
    CAST(bhckb049 AS DOUBLE) AS bhckb049,
    CAST(bhckb052 AS DOUBLE) AS bhckb052,
    CAST(bhckb053 AS DOUBLE) AS bhckb053,
    CAST(bhckb056 AS DOUBLE) AS bhckb056,
    CAST(bhckb491 AS DOUBLE) AS bhckb491,
    CAST(bhckb507 AS DOUBLE) AS bhckb507,
    CAST(bhckb513 AS DOUBLE) AS bhckb513,
    CAST(bhckb515 AS DOUBLE) AS bhckb515,
    CAST(bhckb517 AS DOUBLE) AS bhckb517,
    CAST(bhckb541 AS DOUBLE) AS bhckb541,
    CAST(bhckb558 AS DOUBLE) AS bhckb558,
    CAST(bhckb589 AS DOUBLE) AS bhckb589,
    CAST(bhckb696 AS DOUBLE) AS bhckb696,
    CAST(bhckb697 AS DOUBLE) AS bhckb697,
    CAST(bhckb698 AS DOUBLE) AS bhckb698,
    CAST(bhckb699 AS DOUBLE) AS bhckb699,
    CAST(bhckb700 AS DOUBLE) AS bhckb700,
    CAST(bhckb701 AS DOUBLE) AS bhckb701,
    CAST(bhckb702 AS DOUBLE) AS bhckb702,
    CAST(bhckb703 AS DOUBLE) AS bhckb703,
    CAST(bhckb704 AS DOUBLE) AS bhckb704,
    CAST(bhckb705 AS DOUBLE) AS bhckb705,
    CAST(bhckb706 AS DOUBLE) AS bhckb706,
    CAST(bhckb707 AS DOUBLE) AS bhckb707,
    CAST(bhckb708 AS DOUBLE) AS bhckb708,
    CAST(bhckb709 AS DOUBLE) AS bhckb709,
    CAST(bhckb710 AS DOUBLE) AS bhckb710,
    CAST(bhckb711 AS DOUBLE) AS bhckb711,
    CAST(bhckb712 AS DOUBLE) AS bhckb712,
    CAST(bhckb713 AS DOUBLE) AS bhckb713,
    CAST(bhckb714 AS DOUBLE) AS bhckb714,
    CAST(bhckb715 AS DOUBLE) AS bhckb715,
    CAST(bhckb716 AS DOUBLE) AS bhckb716,
    CAST(bhckb717 AS DOUBLE) AS bhckb717,
    CAST(bhckb718 AS DOUBLE) AS bhckb718,
    CAST(bhckb719 AS DOUBLE) AS bhckb719,
    CAST(bhckb720 AS DOUBLE) AS bhckb720,
    CAST(bhckb721 AS DOUBLE) AS bhckb721,
    CAST(bhckb722 AS DOUBLE) AS bhckb722,
    CAST(bhckb723 AS DOUBLE) AS bhckb723,
    CAST(bhckb724 AS DOUBLE) AS bhckb724,
    CAST(bhckb725 AS DOUBLE) AS bhckb725,
    CAST(bhckb726 AS DOUBLE) AS bhckb726,
    CAST(bhckb727 AS DOUBLE) AS bhckb727,
    CAST(bhckb728 AS DOUBLE) AS bhckb728,
    CAST(bhckb729 AS DOUBLE) AS bhckb729,
    CAST(bhckb730 AS DOUBLE) AS bhckb730,
    CAST(bhckb731 AS DOUBLE) AS bhckb731,
    CAST(bhckb732 AS DOUBLE) AS bhckb732,
    CAST(bhckb733 AS DOUBLE) AS bhckb733,
    CAST(bhckb734 AS DOUBLE) AS bhckb734,
    CAST(bhckb735 AS DOUBLE) AS bhckb735,
    CAST(bhckb736 AS DOUBLE) AS bhckb736,
    CAST(bhckb737 AS DOUBLE) AS bhckb737,
    CAST(bhckb738 AS DOUBLE) AS bhckb738,
    CAST(bhckb739 AS DOUBLE) AS bhckb739,
    CAST(bhckb740 AS DOUBLE) AS bhckb740,
    CAST(bhckb741 AS DOUBLE) AS bhckb741,
    CAST(bhckb742 AS DOUBLE) AS bhckb742,
    CAST(bhckb743 AS DOUBLE) AS bhckb743,
    CAST(bhckb744 AS DOUBLE) AS bhckb744,
    CAST(bhckb745 AS DOUBLE) AS bhckb745,
    CAST(bhckb746 AS DOUBLE) AS bhckb746,
    CAST(bhckb754 AS DOUBLE) AS bhckb754,
    CAST(bhckb755 AS DOUBLE) AS bhckb755,
    CAST(bhckb756 AS DOUBLE) AS bhckb756,
    CAST(bhckb757 AS DOUBLE) AS bhckb757,
    CAST(bhckb758 AS DOUBLE) AS bhckb758,
    CAST(bhckb759 AS DOUBLE) AS bhckb759,
    CAST(bhckb760 AS DOUBLE) AS bhckb760,
    CAST(bhckb764 AS DOUBLE) AS bhckb764,
    CAST(bhckb765 AS DOUBLE) AS bhckb765,
    CAST(bhckb766 AS DOUBLE) AS bhckb766,
    CAST(bhckb767 AS DOUBLE) AS bhckb767,
    CAST(bhckb768 AS DOUBLE) AS bhckb768,
    CAST(bhckb769 AS DOUBLE) AS bhckb769,
    CAST(bhckb773 AS DOUBLE) AS bhckb773,
    CAST(bhckb774 AS DOUBLE) AS bhckb774,
    CAST(bhckb775 AS DOUBLE) AS bhckb775,
    CAST(bhckb783 AS DOUBLE) AS bhckb783,
    CAST(bhckb784 AS DOUBLE) AS bhckb784,
    CAST(bhckb785 AS DOUBLE) AS bhckb785,
    CAST(bhckb786 AS DOUBLE) AS bhckb786,
    CAST(bhckb787 AS DOUBLE) AS bhckb787,
    CAST(bhckb788 AS DOUBLE) AS bhckb788,
    CAST(bhckb789 AS DOUBLE) AS bhckb789,
    CAST(bhckb804 AS DOUBLE) AS bhckb804,
    CAST(bhckb805 AS DOUBLE) AS bhckb805,
    CAST(bhckb808 AS DOUBLE) AS bhckb808,
    CAST(bhckb809 AS DOUBLE) AS bhckb809,
    CAST(bhckb982 AS DOUBLE) AS bhckb982,
    CAST(bhckb989 AS DOUBLE) AS bhckb989,
    CAST(bhckb995 AS DOUBLE) AS bhckb995,
    CAST(bhckb997 AS DOUBLE) AS bhckb997,
    CAST(bhckc015 AS DOUBLE) AS bhckc015,
    CAST(bhckc018 AS DOUBLE) AS bhckc018,
    CAST(bhckc026 AS DOUBLE) AS bhckc026,
    CAST(bhckc027 AS DOUBLE) AS bhckc027,
    CAST(bhckc217 AS DOUBLE) AS bhckc217,
    CAST(bhckc218 AS DOUBLE) AS bhckc218,
    CAST(bhckc227 AS DOUBLE) AS bhckc227,
    CAST(bhckc242 AS DOUBLE) AS bhckc242,
    CAST(bhckc244 AS DOUBLE) AS bhckc244,
    CAST(bhckc245 AS DOUBLE) AS bhckc245,
    CAST(bhckc247 AS DOUBLE) AS bhckc247,
    CAST(bhckc248 AS DOUBLE) AS bhckc248,
    CAST(bhckc249 AS DOUBLE) AS bhckc249,
    CAST(bhckc388 AS DOUBLE) AS bhckc388,
    CAST(bhckc389 AS DOUBLE) AS bhckc389,
    CAST(bhckc391 AS DOUBLE) AS bhckc391,
    CAST(bhckc393 AS DOUBLE) AS bhckc393,
    CAST(bhckc394 AS DOUBLE) AS bhckc394,
    CAST(bhckc395 AS DOUBLE) AS bhckc395,
    CAST(bhckc396 AS DOUBLE) AS bhckc396,
    CAST(bhckc397 AS DOUBLE) AS bhckc397,
    CAST(bhckc398 AS DOUBLE) AS bhckc398,
    CAST(bhckc399 AS DOUBLE) AS bhckc399,
    CAST(bhckc400 AS DOUBLE) AS bhckc400,
    CAST(bhckc401 AS DOUBLE) AS bhckc401,
    CAST(bhckc402 AS DOUBLE) AS bhckc402,
    CAST(bhckc403 AS DOUBLE) AS bhckc403,
    CAST(bhckc404 AS DOUBLE) AS bhckc404,
    CAST(bhckc405 AS DOUBLE) AS bhckc405,
    CAST(bhckc406 AS DOUBLE) AS bhckc406,
    CAST(bhckc407 AS DOUBLE) AS bhckc407,
    CAST(bhckc408 AS DOUBLE) AS bhckc408,
    CAST(bhckc409 AS DOUBLE) AS bhckc409,
    CAST(bhckc502 AS DOUBLE) AS bhckc502,
    CAST(bhckc699 AS DOUBLE) AS bhckc699,
    CAST(bhckc779 AS DOUBLE) AS bhckc779,
    CAST(bhckc780 AS DOUBLE) AS bhckc780,
    CAST(bhckc866 AS DOUBLE) AS bhckc866,
    CAST(bhckc867 AS DOUBLE) AS bhckc867,
    CAST(bhckc868 AS DOUBLE) AS bhckc868,
    CAST(bhckd957 AS DOUBLE) AS bhckd957,
    CAST(bhckd961 AS DOUBLE) AS bhckd961,
    CAST(bhckd966 AS DOUBLE) AS bhckd966,
    CAST(bhckd976 AS DOUBLE) AS bhckd976,
    CAST(bhckd977 AS DOUBLE) AS bhckd977,
    CAST(bhckd978 AS DOUBLE) AS bhckd978,
    CAST(bhckd979 AS DOUBLE) AS bhckd979,
    CAST(bhckd980 AS DOUBLE) AS bhckd980,
    CAST(bhckd981 AS DOUBLE) AS bhckd981,
    CAST(bhckd987 AS DOUBLE) AS bhckd987,
    CAST(bhckd988 AS DOUBLE) AS bhckd988,
    CAST(bhckd989 AS DOUBLE) AS bhckd989,
    CAST(bhckd990 AS DOUBLE) AS bhckd990,
    CAST(bhckd997 AS DOUBLE) AS bhckd997,
    CAST(bhckd998 AS DOUBLE) AS bhckd998,
    CAST(bhckd999 AS DOUBLE) AS bhckd999,
    CAST(bhckf064 AS DOUBLE) AS bhckf064,
    CAST(bhckf065 AS DOUBLE) AS bhckf065,
    CAST(bhckf066 AS DOUBLE) AS bhckf066,
    CAST(bhckf067 AS DOUBLE) AS bhckf067,
    CAST(bhckf068 AS DOUBLE) AS bhckf068,
    CAST(bhckf069 AS DOUBLE) AS bhckf069,
    CAST(bhckf186 AS DOUBLE) AS bhckf186,
    CAST(bhckf187 AS DOUBLE) AS bhckf187,
    CAST(bhckf188 AS DOUBLE) AS bhckf188,
    CAST(bhckf230 AS DOUBLE) AS bhckf230,
    CAST(bhckf231 AS DOUBLE) AS bhckf231,
    CAST(bhckf232 AS DOUBLE) AS bhckf232,
    CAST(bhckf240 AS DOUBLE) AS bhckf240,
    CAST(bhckf243 AS DOUBLE) AS bhckf243,
    CAST(bhckf246 AS DOUBLE) AS bhckf246,
    CAST(bhckf249 AS DOUBLE) AS bhckf249,
    CAST(bhckf252 AS DOUBLE) AS bhckf252,
    CAST(bhckf255 AS DOUBLE) AS bhckf255,
    CAST(bhckf258 AS DOUBLE) AS bhckf258,
    CAST(bhckf261 AS DOUBLE) AS bhckf261,
    CAST(bhckf559 AS DOUBLE) AS bhckf559,
    CAST(bhckf597 AS DOUBLE) AS bhckf597,
    CAST(bhckf598 AS DOUBLE) AS bhckf598,
    CAST(bhckf599 AS DOUBLE) AS bhckf599,
    CAST(bhckf600 AS DOUBLE) AS bhckf600,
    CAST(bhckf601 AS DOUBLE) AS bhckf601,
    CAST(bhckf609 AS DOUBLE) AS bhckf609,
    CAST(bhckf610 AS DOUBLE) AS bhckf610,
    CAST(bhckf614 AS DOUBLE) AS bhckf614,
    CAST(bhckf615 AS DOUBLE) AS bhckf615,
    CAST(bhckf616 AS DOUBLE) AS bhckf616,
    CAST(bhckf617 AS DOUBLE) AS bhckf617,
    CAST(bhckf618 AS DOUBLE) AS bhckf618,
    CAST(bhckf624 AS DOUBLE) AS bhckf624,
    CAST(bhckf632 AS DOUBLE) AS bhckf632,
    CAST(bhckf633 AS DOUBLE) AS bhckf633,
    CAST(bhckf634 AS DOUBLE) AS bhckf634,
    CAST(bhckf635 AS DOUBLE) AS bhckf635,
    CAST(bhckf636 AS DOUBLE) AS bhckf636,
    CAST(bhckf641 AS DOUBLE) AS bhckf641,
    CAST(bhckf642 AS DOUBLE) AS bhckf642,
    CAST(bhckf643 AS DOUBLE) AS bhckf643,
    CAST(bhckf644 AS DOUBLE) AS bhckf644,
    CAST(bhckf645 AS DOUBLE) AS bhckf645,
    CAST(bhckf646 AS DOUBLE) AS bhckf646,
    CAST(bhckf647 AS DOUBLE) AS bhckf647,
    CAST(bhckf648 AS DOUBLE) AS bhckf648,
    CAST(bhckf649 AS DOUBLE) AS bhckf649,
    CAST(bhckf650 AS DOUBLE) AS bhckf650,
    CAST(bhckf651 AS DOUBLE) AS bhckf651,
    CAST(bhckf652 AS DOUBLE) AS bhckf652,
    CAST(bhckf653 AS DOUBLE) AS bhckf653,
    CAST(bhckf654 AS DOUBLE) AS bhckf654,
    CAST(bhckf656 AS DOUBLE) AS bhckf656,
    CAST(bhckf657 AS DOUBLE) AS bhckf657,
    CAST(bhckf659 AS DOUBLE) AS bhckf659,
    CAST(bhckf660 AS DOUBLE) AS bhckf660,
    CAST(bhckf667 AS DOUBLE) AS bhckf667,
    CAST(bhckf668 AS DOUBLE) AS bhckf668,
    CAST(bhckf669 AS DOUBLE) AS bhckf669,
    CAST(bhckf699 AS DOUBLE) AS bhckf699,
    CAST(bhckf790 AS DOUBLE) AS bhckf790,
    CAST(bhckf837 AS DOUBLE) AS bhckf837,
    CAST(bhckf838 AS DOUBLE) AS bhckf838,
    CAST(bhckf842 AS BOOLEAN) AS bhckf842,
    CAST(bhckft04 AS DOUBLE) AS bhckft04,
    CAST(bhckft05 AS DOUBLE) AS bhckft05,
    CAST(bhckg105 AS DOUBLE) AS bhckg105,
    CAST(bhckg214 AS DOUBLE) AS bhckg214,
    CAST(bhckg215 AS DOUBLE) AS bhckg215,
    CAST(bhckg216 AS DOUBLE) AS bhckg216,
    CAST(bhckg217 AS DOUBLE) AS bhckg217,
    CAST(bhckg219 AS DOUBLE) AS bhckg219,
    CAST(bhckg220 AS DOUBLE) AS bhckg220,
    CAST(bhckg222 AS DOUBLE) AS bhckg222,
    CAST(bhckg299 AS DOUBLE) AS bhckg299,
    CAST(bhckg332 AS DOUBLE) AS bhckg332,
    CAST(bhckg333 AS DOUBLE) AS bhckg333,
    CAST(bhckg334 AS DOUBLE) AS bhckg334,
    CAST(bhckg335 AS DOUBLE) AS bhckg335,
    CAST(bhckg348 AS DOUBLE) AS bhckg348,
    CAST(bhckg349 AS DOUBLE) AS bhckg349,
    CAST(bhckg350 AS DOUBLE) AS bhckg350,
    CAST(bhckg351 AS DOUBLE) AS bhckg351,
    CAST(bhckg352 AS DOUBLE) AS bhckg352,
    CAST(bhckg353 AS DOUBLE) AS bhckg353,
    CAST(bhckg354 AS DOUBLE) AS bhckg354,
    CAST(bhckg355 AS DOUBLE) AS bhckg355,
    CAST(bhckg356 AS DOUBLE) AS bhckg356,
    CAST(bhckg357 AS DOUBLE) AS bhckg357,
    CAST(bhckg358 AS DOUBLE) AS bhckg358,
    CAST(bhckg359 AS DOUBLE) AS bhckg359,
    CAST(bhckg360 AS DOUBLE) AS bhckg360,
    CAST(bhckg361 AS DOUBLE) AS bhckg361,
    CAST(bhckg362 AS DOUBLE) AS bhckg362,
    CAST(bhckg363 AS DOUBLE) AS bhckg363,
    CAST(bhckg364 AS DOUBLE) AS bhckg364,
    CAST(bhckg365 AS DOUBLE) AS bhckg365,
    CAST(bhckg366 AS DOUBLE) AS bhckg366,
    CAST(bhckg367 AS DOUBLE) AS bhckg367,
    CAST(bhckg368 AS DOUBLE) AS bhckg368,
    CAST(bhckg369 AS DOUBLE) AS bhckg369,
    CAST(bhckg370 AS DOUBLE) AS bhckg370,
    CAST(bhckg371 AS DOUBLE) AS bhckg371,
    CAST(bhckg372 AS DOUBLE) AS bhckg372,
    CAST(bhckg373 AS DOUBLE) AS bhckg373,
    CAST(bhckg374 AS DOUBLE) AS bhckg374,
    CAST(bhckg375 AS DOUBLE) AS bhckg375,
    CAST(bhckg378 AS DOUBLE) AS bhckg378,
    CAST(bhckg379 AS DOUBLE) AS bhckg379,
    CAST(bhckg380 AS DOUBLE) AS bhckg380,
    CAST(bhckg381 AS DOUBLE) AS bhckg381,
    CAST(bhckg382 AS DOUBLE) AS bhckg382,
    CAST(bhckg383 AS DOUBLE) AS bhckg383,
    CAST(bhckg384 AS DOUBLE) AS bhckg384,
    CAST(bhckg385 AS DOUBLE) AS bhckg385,
    CAST(bhckg386 AS DOUBLE) AS bhckg386,
    CAST(bhckg387 AS DOUBLE) AS bhckg387,
    CAST(bhckg388 AS DOUBLE) AS bhckg388,
    CAST(bhckg418 AS DOUBLE) AS bhckg418,
    CAST(bhckg419 AS DOUBLE) AS bhckg419,
    CAST(bhckg420 AS DOUBLE) AS bhckg420,
    CAST(bhckg421 AS DOUBLE) AS bhckg421,
    CAST(bhckg422 AS DOUBLE) AS bhckg422,
    CAST(bhckg423 AS DOUBLE) AS bhckg423,
    CAST(bhckg424 AS DOUBLE) AS bhckg424,
    CAST(bhckg425 AS DOUBLE) AS bhckg425,
    CAST(bhckg426 AS DOUBLE) AS bhckg426,
    CAST(bhckg427 AS DOUBLE) AS bhckg427,
    CAST(bhckg428 AS DOUBLE) AS bhckg428,
    CAST(bhckg429 AS DOUBLE) AS bhckg429,
    CAST(bhckg430 AS DOUBLE) AS bhckg430,
    CAST(bhckg431 AS DOUBLE) AS bhckg431,
    CAST(bhckg432 AS DOUBLE) AS bhckg432,
    CAST(bhckg433 AS DOUBLE) AS bhckg433,
    CAST(bhckg434 AS DOUBLE) AS bhckg434,
    CAST(bhckg435 AS DOUBLE) AS bhckg435,
    CAST(bhckg436 AS DOUBLE) AS bhckg436,
    CAST(bhckg437 AS DOUBLE) AS bhckg437,
    CAST(bhckg438 AS DOUBLE) AS bhckg438,
    CAST(bhckg439 AS DOUBLE) AS bhckg439,
    CAST(bhckg440 AS DOUBLE) AS bhckg440,
    CAST(bhckg441 AS DOUBLE) AS bhckg441,
    CAST(bhckg442 AS DOUBLE) AS bhckg442,
    CAST(bhckg443 AS DOUBLE) AS bhckg443,
    CAST(bhckg444 AS DOUBLE) AS bhckg444,
    CAST(bhckg445 AS DOUBLE) AS bhckg445,
    CAST(bhckg446 AS DOUBLE) AS bhckg446,
    CAST(bhckg447 AS DOUBLE) AS bhckg447,
    CAST(bhckg448 AS DOUBLE) AS bhckg448,
    CAST(bhckg449 AS DOUBLE) AS bhckg449,
    CAST(bhckg450 AS DOUBLE) AS bhckg450,
    CAST(bhckg451 AS DOUBLE) AS bhckg451,
    CAST(bhckg452 AS DOUBLE) AS bhckg452,
    CAST(bhckg453 AS DOUBLE) AS bhckg453,
    CAST(bhckg454 AS DOUBLE) AS bhckg454,
    CAST(bhckg455 AS DOUBLE) AS bhckg455,
    CAST(bhckg456 AS DOUBLE) AS bhckg456,
    CAST(bhckg457 AS DOUBLE) AS bhckg457,
    CAST(bhckg458 AS DOUBLE) AS bhckg458,
    CAST(bhckg459 AS DOUBLE) AS bhckg459,
    CAST(bhckg460 AS DOUBLE) AS bhckg460,
    CAST(bhckg461 AS DOUBLE) AS bhckg461,
    CAST(bhckg462 AS DOUBLE) AS bhckg462,
    CAST(bhckg493 AS DOUBLE) AS bhckg493,
    CAST(bhckg494 AS DOUBLE) AS bhckg494,
    CAST(bhckg495 AS DOUBLE) AS bhckg495,
    CAST(bhckg496 AS DOUBLE) AS bhckg496,
    CAST(bhckg497 AS DOUBLE) AS bhckg497,
    CAST(bhckg498 AS DOUBLE) AS bhckg498,
    CAST(bhckg499 AS DOUBLE) AS bhckg499,
    CAST(bhckg500 AS DOUBLE) AS bhckg500,
    CAST(bhckg501 AS DOUBLE) AS bhckg501,
    CAST(bhckg502 AS DOUBLE) AS bhckg502,
    CAST(bhckg503 AS DOUBLE) AS bhckg503,
    CAST(bhckg504 AS DOUBLE) AS bhckg504,
    CAST(bhckg505 AS DOUBLE) AS bhckg505,
    CAST(bhckg506 AS DOUBLE) AS bhckg506,
    CAST(bhckg512 AS DOUBLE) AS bhckg512,
    CAST(bhckg513 AS DOUBLE) AS bhckg513,
    CAST(bhckg514 AS DOUBLE) AS bhckg514,
    CAST(bhckg515 AS DOUBLE) AS bhckg515,
    CAST(bhckg516 AS DOUBLE) AS bhckg516,
    CAST(bhckg517 AS DOUBLE) AS bhckg517,
    CAST(bhckg518 AS DOUBLE) AS bhckg518,
    CAST(bhckg519 AS DOUBLE) AS bhckg519,
    CAST(bhckg520 AS DOUBLE) AS bhckg520,
    CAST(bhckg526 AS DOUBLE) AS bhckg526,
    CAST(bhckg527 AS DOUBLE) AS bhckg527,
    CAST(bhckg528 AS DOUBLE) AS bhckg528,
    CAST(bhckg529 AS DOUBLE) AS bhckg529,
    CAST(bhckg530 AS DOUBLE) AS bhckg530,
    CAST(bhckg531 AS DOUBLE) AS bhckg531,
    CAST(bhckg532 AS DOUBLE) AS bhckg532,
    CAST(bhckg533 AS DOUBLE) AS bhckg533,
    CAST(bhckg534 AS DOUBLE) AS bhckg534,
    CAST(bhckg535 AS DOUBLE) AS bhckg535,
    CAST(bhckg551 AS DOUBLE) AS bhckg551,
    CAST(bhckg552 AS DOUBLE) AS bhckg552,
    CAST(bhckg553 AS DOUBLE) AS bhckg553,
    CAST(bhckg554 AS DOUBLE) AS bhckg554,
    CAST(bhckg555 AS DOUBLE) AS bhckg555,
    CAST(bhckg556 AS DOUBLE) AS bhckg556,
    CAST(bhckg557 AS DOUBLE) AS bhckg557,
    CAST(bhckg558 AS DOUBLE) AS bhckg558,
    CAST(bhckg559 AS DOUBLE) AS bhckg559,
    CAST(bhckg560 AS DOUBLE) AS bhckg560,
    CAST(bhckg576 AS DOUBLE) AS bhckg576,
    CAST(bhckg577 AS DOUBLE) AS bhckg577,
    CAST(bhckg578 AS DOUBLE) AS bhckg578,
    CAST(bhckg579 AS DOUBLE) AS bhckg579,
    CAST(bhckg580 AS DOUBLE) AS bhckg580,
    CAST(bhckg581 AS DOUBLE) AS bhckg581,
    CAST(bhckg582 AS DOUBLE) AS bhckg582,
    CAST(bhckg583 AS DOUBLE) AS bhckg583,
    CAST(bhckg584 AS DOUBLE) AS bhckg584,
    CAST(bhckg585 AS DOUBLE) AS bhckg585,
    CAST(bhckg591 AS DOUBLE) AS bhckg591,
    CAST(bhckg603 AS DOUBLE) AS bhckg603,
    CAST(bhckg604 AS DOUBLE) AS bhckg604,
    CAST(bhckg605 AS DOUBLE) AS bhckg605,
    CAST(bhckg612 AS DOUBLE) AS bhckg612,
    CAST(bhckg613 AS DOUBLE) AS bhckg613,
    CAST(bhckg614 AS DOUBLE) AS bhckg614,
    CAST(bhckg615 AS DOUBLE) AS bhckg615,
    CAST(bhckg616 AS DOUBLE) AS bhckg616,
    CAST(bhckg617 AS DOUBLE) AS bhckg617,
    CAST(bhckg624 AS DOUBLE) AS bhckg624,
    CAST(bhckg625 AS DOUBLE) AS bhckg625,
    CAST(bhckg626 AS DOUBLE) AS bhckg626,
    CAST(bhckg627 AS DOUBLE) AS bhckg627,
    CAST(bhckg628 AS DOUBLE) AS bhckg628,
    CAST(bhckg629 AS DOUBLE) AS bhckg629,
    CAST(bhckg630 AS DOUBLE) AS bhckg630,
    CAST(bhckg631 AS DOUBLE) AS bhckg631,
    CAST(bhckg632 AS DOUBLE) AS bhckg632,
    CAST(bhckg633 AS DOUBLE) AS bhckg633,
    CAST(bhckg634 AS DOUBLE) AS bhckg634,
    CAST(bhckg635 AS DOUBLE) AS bhckg635,
    CAST(bhckg636 AS DOUBLE) AS bhckg636,
    CAST(bhckg637 AS DOUBLE) AS bhckg637,
    CAST(bhckg641 AS DOUBLE) AS bhckg641,
    CAST(bhckg651 AS DOUBLE) AS bhckg651,
    CAST(bhckg652 AS DOUBLE) AS bhckg652,
    CAST(bhckh171 AS DOUBLE) AS bhckh171,
    CAST(bhckh191 AS DOUBLE) AS bhckh191,
    CAST(bhckh289 AS DOUBLE) AS bhckh289,
    CAST(bhckh290 AS DOUBLE) AS bhckh290,
    CAST(bhckh291 AS DOUBLE) AS bhckh291,
    CAST(bhckh292 AS DOUBLE) AS bhckh292,
    CAST(bhckh300 AS DOUBLE) AS bhckh300,
    CAST(bhckh301 AS DOUBLE) AS bhckh301,
    CAST(bhckh302 AS DOUBLE) AS bhckh302,
    CAST(bhckh303 AS DOUBLE) AS bhckh303,
    CAST(bhckh304 AS DOUBLE) AS bhckh304,
    CAST(bhckh307 AS DOUBLE) AS bhckh307,
    CAST(bhckh308 AS DOUBLE) AS bhckh308,
    CAST(bhckh309 AS DOUBLE) AS bhckh309,
    CAST(bhckh310 AS DOUBLE) AS bhckh310,
    CAST(bhckhj74 AS DOUBLE) AS bhckhj74,
    CAST(bhckhj75 AS DOUBLE) AS bhckhj75,
    CAST(bhckhj76 AS DOUBLE) AS bhckhj76,
    CAST(bhckhj77 AS DOUBLE) AS bhckhj77,
    CAST(bhckhj86 AS DOUBLE) AS bhckhj86,
    CAST(bhckhj87 AS DOUBLE) AS bhckhj87,
    CAST(bhckhj90 AS DOUBLE) AS bhckhj90,
    CAST(bhckhj91 AS DOUBLE) AS bhckhj91,
    CAST(bhckhj96 AS DOUBLE) AS bhckhj96,
    CAST(bhckhj97 AS DOUBLE) AS bhckhj97,
    CAST(bhckhj98 AS DOUBLE) AS bhckhj98,
    CAST(bhckhj99 AS DOUBLE) AS bhckhj99,
    CAST(bhckhk00 AS DOUBLE) AS bhckhk00,
    CAST(bhckhk01 AS DOUBLE) AS bhckhk01,
    CAST(bhckhk25 AS DOUBLE) AS bhckhk25,
    CAST(bhckhk26 AS DOUBLE) AS bhckhk26,
    CAST(bhckhk27 AS DOUBLE) AS bhckhk27,
    CAST(bhckhk28 AS DOUBLE) AS bhckhk28,
    CAST(bhckht50 AS DOUBLE) AS bhckht50,
    CAST(bhckht51 AS DOUBLE) AS bhckht51,
    CAST(bhckht52 AS DOUBLE) AS bhckht52,
    CAST(bhckht53 AS DOUBLE) AS bhckht53,
    CAST(bhckht66 AS DOUBLE) AS bhckht66,
    CAST(bhckht67 AS DOUBLE) AS bhckht67,
    CAST(bhckht68 AS DOUBLE) AS bhckht68,
    CAST(bhckht70 AS DOUBLE) AS bhckht70,
    CAST(bhckht81 AS DOUBLE) AS bhckht81,
    CAST(bhckht82 AS DOUBLE) AS bhckht82,
    CAST(bhckht86 AS DOUBLE) AS bhckht86,
    CAST(bhckhu16 AS DOUBLE) AS bhckhu16,
    CAST(bhckhu17 AS DOUBLE) AS bhckhu17,
    CAST(bhckhu18 AS DOUBLE) AS bhckhu18,
    CAST(bhckj319 AS DOUBLE) AS bhckj319,
    CAST(bhckj321 AS DOUBLE) AS bhckj321,
    CAST(bhckj457 AS DOUBLE) AS bhckj457,
    CAST(bhckj458 AS DOUBLE) AS bhckj458,
    CAST(bhckj459 AS DOUBLE) AS bhckj459,
    CAST(bhckjf77 AS DOUBLE) AS bhckjf77,
    CAST(bhckjf78 AS DOUBLE) AS bhckjf78,
    CAST(bhckjh89 AS DOUBLE) AS bhckjh89,
    CAST(bhckjh90 AS DOUBLE) AS bhckjh90,
    CAST(bhckjh95 AS DOUBLE) AS bhckjh95,
    CAST(bhckjh96 AS DOUBLE) AS bhckjh96,
    CAST(bhckjj02 AS DOUBLE) AS bhckjj02,
    CAST(bhckjj33 AS DOUBLE) AS bhckjj33,
    CAST(bhckk042 AS DOUBLE) AS bhckk042,
    CAST(bhckk043 AS DOUBLE) AS bhckk043,
    CAST(bhckk044 AS DOUBLE) AS bhckk044,
    CAST(bhckk102 AS DOUBLE) AS bhckk102,
    CAST(bhckk103 AS DOUBLE) AS bhckk103,
    CAST(bhckk104 AS DOUBLE) AS bhckk104,
    CAST(bhckk133 AS DOUBLE) AS bhckk133,
    CAST(bhckk141 AS DOUBLE) AS bhckk141,
    CAST(bhckk195 AS DOUBLE) AS bhckk195,
    CAST(bhckk197 AS DOUBLE) AS bhckk197,
    CAST(bhckk198 AS DOUBLE) AS bhckk198,
    CAST(bhckk199 AS DOUBLE) AS bhckk199,
    CAST(bhckk200 AS DOUBLE) AS bhckk200,
    CAST(bhckk206 AS DOUBLE) AS bhckk206,
    CAST(bhckk209 AS DOUBLE) AS bhckk209,
    CAST(bhckk210 AS DOUBLE) AS bhckk210,
    CAST(bhckk211 AS DOUBLE) AS bhckk211,
    CAST(bhckkx48 AS DOUBLE) AS bhckkx48,
    CAST(bhckkx49 AS DOUBLE) AS bhckkx49,
    CAST(bhckkx56 AS DOUBLE) AS bhckkx56,
    CAST(bhckkx59 AS DOUBLE) AS bhckkx59,
    CAST(bhckkx66 AS DOUBLE) AS bhckkx66,
    CAST(bhckkx67 AS DOUBLE) AS bhckkx67,
    CAST(bhckkx68 AS DOUBLE) AS bhckkx68,
    CAST(bhckl183 AS DOUBLE) AS bhckl183,
    CAST(bhckl184 AS DOUBLE) AS bhckl184,
    CAST(bhckl185 AS DOUBLE) AS bhckl185,
    CAST(bhckl186 AS DOUBLE) AS bhckl186,
    CAST(bhckl187 AS DOUBLE) AS bhckl187,
    CAST(bhckl188 AS DOUBLE) AS bhckl188,
    CAST(bhckl191 AS BOOLEAN) AS bhckl191,
    CAST(bhckl192 AS BOOLEAN) AS bhckl192,
    CAST(bhckle75 AS DOUBLE) AS bhckle75,
    CAST(bhcklg25 AS BOOLEAN) AS bhcklg25,
    CAST(bhcklg27 AS DOUBLE) AS bhcklg27,
    CAST(bhcklg28 AS DOUBLE) AS bhcklg28,
    CAST(bhckll57 AS DOUBLE) AS bhckll57,
    CAST(bhckm288 AS DOUBLE) AS bhckm288,
    CAST(bhckm708 AS DOUBLE) AS bhckm708,
    CAST(bhckm709 AS DOUBLE) AS bhckm709,
    CAST(bhckm710 AS DOUBLE) AS bhckm710,
    CAST(bhckm711 AS DOUBLE) AS bhckm711,
    CAST(bhckm712 AS DOUBLE) AS bhckm712,
    CAST(bhckm713 AS DOUBLE) AS bhckm713,
    CAST(bhckm714 AS DOUBLE) AS bhckm714,
    CAST(bhckm715 AS DOUBLE) AS bhckm715,
    CAST(bhckm716 AS DOUBLE) AS bhckm716,
    CAST(bhckm717 AS DOUBLE) AS bhckm717,
    CAST(bhckm719 AS DOUBLE) AS bhckm719,
    CAST(bhckm720 AS DOUBLE) AS bhckm720,
    CAST(bhckm721 AS DOUBLE) AS bhckm721,
    CAST(bhckm722 AS DOUBLE) AS bhckm722,
    CAST(bhckm723 AS DOUBLE) AS bhckm723,
    CAST(bhckm724 AS DOUBLE) AS bhckm724,
    CAST(bhckm725 AS DOUBLE) AS bhckm725,
    CAST(bhckm726 AS DOUBLE) AS bhckm726,
    CAST(bhckm745 AS DOUBLE) AS bhckm745,
    CAST(bhckm746 AS DOUBLE) AS bhckm746,
    CAST(bhckm747 AS DOUBLE) AS bhckm747,
    CAST(bhckm748 AS DOUBLE) AS bhckm748,
    CAST(bhckm749 AS DOUBLE) AS bhckm749,
    CAST(bhckm750 AS DOUBLE) AS bhckm750,
    CAST(bhckm751 AS DOUBLE) AS bhckm751,
    CAST(bhckmg93 AS DOUBLE) AS bhckmg93,
    CAST(bhckmg95 AS DOUBLE) AS bhckmg95,
    CAST(bhcks413 AS DOUBLE) AS bhcks413,
    CAST(bhcks419 AS DOUBLE) AS bhcks419,
    CAST(bhcks423 AS DOUBLE) AS bhcks423,
    CAST(bhcks431 AS DOUBLE) AS bhcks431,
    CAST(bhcks439 AS DOUBLE) AS bhcks439,
    CAST(bhcks445 AS DOUBLE) AS bhcks445,
    CAST(bhcks449 AS DOUBLE) AS bhcks449,
    CAST(bhcks457 AS DOUBLE) AS bhcks457,
    CAST(bhcks466 AS DOUBLE) AS bhcks466,
    CAST(bhcks467 AS DOUBLE) AS bhcks467,
    CAST(bhcks475 AS DOUBLE) AS bhcks475,
    CAST(bhcks480 AS DOUBLE) AS bhcks480,
    CAST(bhcks485 AS DOUBLE) AS bhcks485,
    CAST(bhcks490 AS DOUBLE) AS bhcks490,
    CAST(bhcks495 AS DOUBLE) AS bhcks495,
    CAST(bhcks500 AS DOUBLE) AS bhcks500,
    CAST(bhcks503 AS DOUBLE) AS bhcks503,
    CAST(bhcks504 AS DOUBLE) AS bhcks504,
    CAST(bhcks505 AS DOUBLE) AS bhcks505,
    CAST(bhcks506 AS DOUBLE) AS bhcks506,
    CAST(bhcks507 AS DOUBLE) AS bhcks507,
    CAST(bhcks510 AS DOUBLE) AS bhcks510,
    CAST(bhcks512 AS DOUBLE) AS bhcks512,
    CAST(bhcks514 AS DOUBLE) AS bhcks514,
    CAST(bhcks515 AS DOUBLE) AS bhcks515,
    CAST(bhcks516 AS DOUBLE) AS bhcks516,
    CAST(bhcks517 AS DOUBLE) AS bhcks517,
    CAST(bhcks518 AS DOUBLE) AS bhcks518,
    CAST(bhcks519 AS DOUBLE) AS bhcks519,
    CAST(bhcks520 AS DOUBLE) AS bhcks520,
    CAST(bhcks521 AS DOUBLE) AS bhcks521,
    CAST(bhcks522 AS DOUBLE) AS bhcks522,
    CAST(bhcks523 AS DOUBLE) AS bhcks523,
    CAST(bhcks525 AS DOUBLE) AS bhcks525,
    CAST(bhcks526 AS DOUBLE) AS bhcks526,
    CAST(bhcks527 AS DOUBLE) AS bhcks527,
    CAST(bhcks528 AS DOUBLE) AS bhcks528,
    CAST(bhcks529 AS DOUBLE) AS bhcks529,
    CAST(bhcks530 AS DOUBLE) AS bhcks530,
    CAST(bhcks531 AS DOUBLE) AS bhcks531,
    CAST(bhcks539 AS DOUBLE) AS bhcks539,
    CAST(bhcks540 AS DOUBLE) AS bhcks540,
    CAST(bhcks541 AS DOUBLE) AS bhcks541,
    CAST(bhcks542 AS DOUBLE) AS bhcks542,
    CAST(bhcks543 AS DOUBLE) AS bhcks543,
    CAST(bhcks544 AS DOUBLE) AS bhcks544,
    CAST(bhcks545 AS DOUBLE) AS bhcks545,
    CAST(bhcks546 AS DOUBLE) AS bhcks546,
    CAST(bhcks547 AS DOUBLE) AS bhcks547,
    CAST(bhcks548 AS DOUBLE) AS bhcks548,
    CAST(bhcks558 AS DOUBLE) AS bhcks558,
    CAST(bhcks559 AS DOUBLE) AS bhcks559,
    CAST(bhcks560 AS DOUBLE) AS bhcks560,
    CAST(bhcks561 AS DOUBLE) AS bhcks561,
    CAST(bhcks562 AS DOUBLE) AS bhcks562,
    CAST(bhcks563 AS DOUBLE) AS bhcks563,
    CAST(bhcks564 AS DOUBLE) AS bhcks564,
    CAST(bhcks565 AS DOUBLE) AS bhcks565,
    CAST(bhcks566 AS DOUBLE) AS bhcks566,
    CAST(bhcks567 AS DOUBLE) AS bhcks567,
    CAST(bhcks568 AS DOUBLE) AS bhcks568,
    CAST(bhcks569 AS DOUBLE) AS bhcks569,
    CAST(bhcks570 AS DOUBLE) AS bhcks570,
    CAST(bhcks571 AS DOUBLE) AS bhcks571,
    CAST(bhcks572 AS DOUBLE) AS bhcks572,
    CAST(bhcks573 AS DOUBLE) AS bhcks573,
    CAST(bhcks574 AS DOUBLE) AS bhcks574,
    CAST(bhcks575 AS DOUBLE) AS bhcks575,
    CAST(bhcks576 AS DOUBLE) AS bhcks576,
    CAST(bhcks577 AS DOUBLE) AS bhcks577,
    CAST(bhcks578 AS DOUBLE) AS bhcks578,
    CAST(bhcks579 AS DOUBLE) AS bhcks579,
    CAST(bhcks580 AS DOUBLE) AS bhcks580,
    CAST(bhcks581 AS DOUBLE) AS bhcks581,
    CAST(bhcks624 AS DOUBLE) AS bhcks624,
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
                let bhck0383 = f("bhck0383");
                let bhck0384 = f("bhck0384");
                let bhck0387 = f("bhck0387");
                let bhck0416 = f("bhck0416");
                let bhck0535 = f("bhck0535");
                let bhck1395 = f("bhck1395");
                let bhck1403 = f("bhck1403");
                let bhck1406 = f("bhck1406");
                let bhck1407 = f("bhck1407");
                let bhck1658 = f("bhck1658");
                let bhck1659 = f("bhck1659");
                let bhck1661 = f("bhck1661");
                let bhck1771 = f("bhck1771");
                let bhck1772 = f("bhck1772");
                let bhck1914 = f("bhck1914");
                let bhck2033 = f("bhck2033");
                let bhck2079 = f("bhck2079");
                let bhck2122 = f("bhck2122");
                let bhck2123 = f("bhck2123");
                let bhck2125 = f("bhck2125");
                let bhck2145 = f("bhck2145");
                let bhck2170 = f("bhck2170");
                let bhck2221 = f("bhck2221");
                let bhck2419 = f("bhck2419");
                let bhck2432 = f("bhck2432");
                let bhck2635 = f("bhck2635");
                let bhck2744 = f("bhck2744");
                let bhck2948 = f("bhck2948");
                let bhck3196 = f("bhck3196");
                let bhck3210 = f("bhck3210");
                let bhck3240 = f("bhck3240");
                let bhck3247 = f("bhck3247");
                let bhck3283 = f("bhck3283");
                let bhck3290 = f("bhck3290");
                let bhck3293 = f("bhck3293");
                let bhck3300 = f("bhck3300");
                let bhck3353 = f("bhck3353");
                let bhck3365 = f("bhck3365");
                let bhck3368 = f("bhck3368");
                let bhck3376 = f("bhck3376");
                let bhck3377 = f("bhck3377");
                let bhck3378 = f("bhck3378");
                let bhck3401 = f("bhck3401");
                let bhck3402 = f("bhck3402");
                let bhck3404 = f("bhck3404");
                let bhck3408 = f("bhck3408");
                let bhck3428 = f("bhck3428");
                let bhck3429 = f("bhck3429");
                let bhck3432 = f("bhck3432");
                let bhck3433 = f("bhck3433");
                let bhck3459 = f("bhck3459");
                let bhck3515 = f("bhck3515");
                let bhck3516 = f("bhck3516");
                let bhck3517 = f("bhck3517");
                let bhck3519 = f("bhck3519");
                let bhck3521 = f("bhck3521");
                let bhck3531 = f("bhck3531");
                let bhck3532 = f("bhck3532");
                let bhck3533 = f("bhck3533");
                let bhck3534 = f("bhck3534");
                let bhck3535 = f("bhck3535");
                let bhck3536 = f("bhck3536");
                let bhck3537 = f("bhck3537");
                let bhck3542 = f("bhck3542");
                let bhck3543 = f("bhck3543");
                let bhck3545 = f("bhck3545");
                let bhck3547 = f("bhck3547");
                let bhck3548 = f("bhck3548");
                let bhck3573 = f("bhck3573");
                let bhck3575 = f("bhck3575");
                let bhck3577 = f("bhck3577");
                let bhck3579 = f("bhck3579");
                let bhck3583 = f("bhck3583");
                let bhck3585 = f("bhck3585");
                let bhck3589 = f("bhck3589");
                let bhck3591 = f("bhck3591");
                let bhck3792 = f("bhck3792");
                let bhck3814 = f("bhck3814");
                let bhck3815 = f("bhck3815");
                let bhck3817 = f("bhck3817");
                let bhck3818 = f("bhck3818");
                let bhck4062 = f("bhck4062");
                let bhck4073 = f("bhck4073");
                let bhck4079 = f("bhck4079");
                let bhck4093 = f("bhck4093");
                let bhck4107 = f("bhck4107");
                let bhck4135 = f("bhck4135");
                let bhck4230 = f("bhck4230");
                let bhck4243 = f("bhck4243");
                let bhck4307 = f("bhck4307");
                let bhck4483 = f("bhck4483");
                let bhck4505 = f("bhck4505");
                let bhck4605 = f("bhck4605");
                let bhck4617 = f("bhck4617");
                let bhck4618 = f("bhck4618");
                let bhck4627 = f("bhck4627");
                let bhck4628 = f("bhck4628");
                let bhck4661 = f("bhck4661");
                let bhck4662 = f("bhck4662");
                let bhck4663 = f("bhck4663");
                let bhck4664 = f("bhck4664");
                let bhck4665 = f("bhck4665");
                let bhck4666 = f("bhck4666");
                let bhck4667 = f("bhck4667");
                let bhck4668 = f("bhck4668");
                let bhck4669 = f("bhck4669");
                let bhck4782 = f("bhck4782");
                let bhck4783 = f("bhck4783");
                let bhck5306 = f("bhck5306");
                let bhck5311 = f("bhck5311");
                let bhck5352 = f("bhck5352");
                let bhck5353 = f("bhck5353");
                let bhck5357 = f("bhck5357");
                let bhck5358 = f("bhck5358");
                let bhck5376 = f("bhck5376");
                let bhck5396 = f("bhck5396");
                let bhck5410 = f("bhck5410");
                let bhck5412 = f("bhck5412");
                let bhck5414 = f("bhck5414");
                let bhck5479 = f("bhck5479");
                let bhck5483 = f("bhck5483");
                let bhck5484 = f("bhck5484");
                let bhck5500 = f("bhck5500");
                let bhck5501 = f("bhck5501");
                let bhck5502 = f("bhck5502");
                let bhck5503 = f("bhck5503");
                let bhck5504 = f("bhck5504");
                let bhck5505 = f("bhck5505");
                let bhck5523 = f("bhck5523");
                let bhck5524 = f("bhck5524");
                let bhck5525 = f("bhck5525");
                let bhck5526 = f("bhck5526");
                let bhck5990 = f("bhck5990");
                let bhck6562 = f("bhck6562");
                let bhck6568 = f("bhck6568");
                let bhck6570 = f("bhck6570");
                let bhck6577 = f("bhck6577");
                let bhck6996 = b("bhck6996");
                let bhck6997 = b("bhck6997");
                let bhck7204 = f("bhck7204");
                let bhck7205 = f("bhck7205");
                let bhck7206 = f("bhck7206");
                let bhck8274 = f("bhck8274");
                let bhck8275 = f("bhck8275");
                let bhck8551 = f("bhck8551");
                let bhck8552 = f("bhck8552");
                let bhck8553 = f("bhck8553");
                let bhck8554 = f("bhck8554");
                let bhck8555 = f("bhck8555");
                let bhck8556 = f("bhck8556");
                let bhck8701 = f("bhck8701");
                let bhck8702 = f("bhck8702");
                let bhck8703 = f("bhck8703");
                let bhck8704 = f("bhck8704");
                let bhck8705 = f("bhck8705");
                let bhck8706 = f("bhck8706");
                let bhck8707 = f("bhck8707");
                let bhck8708 = f("bhck8708");
                let bhck8709 = f("bhck8709");
                let bhck8710 = f("bhck8710");
                let bhck8711 = f("bhck8711");
                let bhck8712 = f("bhck8712");
                let bhck8713 = f("bhck8713");
                let bhck8714 = f("bhck8714");
                let bhck8715 = f("bhck8715");
                let bhck8716 = f("bhck8716");
                let bhck8717 = b("bhck8717");
                let bhck8718 = b("bhck8718");
                let bhck8723 = f("bhck8723");
                let bhck8724 = f("bhck8724");
                let bhck8725 = f("bhck8725");
                let bhck8726 = f("bhck8726");
                let bhck8727 = f("bhck8727");
                let bhck8728 = f("bhck8728");
                let bhck8729 = f("bhck8729");
                let bhck8730 = f("bhck8730");
                let bhck8731 = f("bhck8731");
                let bhck8732 = f("bhck8732");
                let bhck8765 = f("bhck8765");
                let bhck8768 = b("bhck8768");
                let bhck8784 = f("bhck8784");
                let bhck8834 = b("bhck8834");
                let bhck8836 = b("bhck8836");
                let bhck8838 = b("bhck8838");
                let bhck9191 = f("bhck9191");
                let bhck9802 = f("bhck9802");
                let bhcka102 = f("bhcka102");
                let bhcka120 = b("bhcka120");
                let bhcka121 = b("bhcka121");
                let bhcka122 = b("bhcka122");
                let bhcka123 = b("bhcka123");
                let bhcka124 = b("bhcka124");
                let bhcka126 = f("bhcka126");
                let bhcka127 = f("bhcka127");
                let bhcka128 = b("bhcka128");
                let bhcka195 = f("bhcka195");
                let bhcka220 = f("bhcka220");
                let bhcka223 = f("bhcka223");
                let bhcka249 = f("bhcka249");
                let bhcka288 = f("bhcka288");
                let bhcka591 = f("bhcka591");
                let bhckb027 = f("bhckb027");
                let bhckb028 = f("bhckb028");
                let bhckb031 = f("bhckb031");
                let bhckb033 = f("bhckb033");
                let bhckb034 = f("bhckb034");
                let bhckb037 = f("bhckb037");
                let bhckb038 = f("bhckb038");
                let bhckb041 = f("bhckb041");
                let bhckb042 = f("bhckb042");
                let bhckb043 = f("bhckb043");
                let bhckb046 = f("bhckb046");
                let bhckb048 = f("bhckb048");
                let bhckb049 = f("bhckb049");
                let bhckb052 = f("bhckb052");
                let bhckb053 = f("bhckb053");
                let bhckb056 = f("bhckb056");
                let bhckb491 = f("bhckb491");
                let bhckb507 = f("bhckb507");
                let bhckb513 = f("bhckb513");
                let bhckb515 = f("bhckb515");
                let bhckb517 = f("bhckb517");
                let bhckb541 = f("bhckb541");
                let bhckb558 = f("bhckb558");
                let bhckb589 = f("bhckb589");
                let bhckb696 = f("bhckb696");
                let bhckb697 = f("bhckb697");
                let bhckb698 = f("bhckb698");
                let bhckb699 = f("bhckb699");
                let bhckb700 = f("bhckb700");
                let bhckb701 = f("bhckb701");
                let bhckb702 = f("bhckb702");
                let bhckb703 = f("bhckb703");
                let bhckb704 = f("bhckb704");
                let bhckb705 = f("bhckb705");
                let bhckb706 = f("bhckb706");
                let bhckb707 = f("bhckb707");
                let bhckb708 = f("bhckb708");
                let bhckb709 = f("bhckb709");
                let bhckb710 = f("bhckb710");
                let bhckb711 = f("bhckb711");
                let bhckb712 = f("bhckb712");
                let bhckb713 = f("bhckb713");
                let bhckb714 = f("bhckb714");
                let bhckb715 = f("bhckb715");
                let bhckb716 = f("bhckb716");
                let bhckb717 = f("bhckb717");
                let bhckb718 = f("bhckb718");
                let bhckb719 = f("bhckb719");
                let bhckb720 = f("bhckb720");
                let bhckb721 = f("bhckb721");
                let bhckb722 = f("bhckb722");
                let bhckb723 = f("bhckb723");
                let bhckb724 = f("bhckb724");
                let bhckb725 = f("bhckb725");
                let bhckb726 = f("bhckb726");
                let bhckb727 = f("bhckb727");
                let bhckb728 = f("bhckb728");
                let bhckb729 = f("bhckb729");
                let bhckb730 = f("bhckb730");
                let bhckb731 = f("bhckb731");
                let bhckb732 = f("bhckb732");
                let bhckb733 = f("bhckb733");
                let bhckb734 = f("bhckb734");
                let bhckb735 = f("bhckb735");
                let bhckb736 = f("bhckb736");
                let bhckb737 = f("bhckb737");
                let bhckb738 = f("bhckb738");
                let bhckb739 = f("bhckb739");
                let bhckb740 = f("bhckb740");
                let bhckb741 = f("bhckb741");
                let bhckb742 = f("bhckb742");
                let bhckb743 = f("bhckb743");
                let bhckb744 = f("bhckb744");
                let bhckb745 = f("bhckb745");
                let bhckb746 = f("bhckb746");
                let bhckb754 = f("bhckb754");
                let bhckb755 = f("bhckb755");
                let bhckb756 = f("bhckb756");
                let bhckb757 = f("bhckb757");
                let bhckb758 = f("bhckb758");
                let bhckb759 = f("bhckb759");
                let bhckb760 = f("bhckb760");
                let bhckb764 = f("bhckb764");
                let bhckb765 = f("bhckb765");
                let bhckb766 = f("bhckb766");
                let bhckb767 = f("bhckb767");
                let bhckb768 = f("bhckb768");
                let bhckb769 = f("bhckb769");
                let bhckb773 = f("bhckb773");
                let bhckb774 = f("bhckb774");
                let bhckb775 = f("bhckb775");
                let bhckb783 = f("bhckb783");
                let bhckb784 = f("bhckb784");
                let bhckb785 = f("bhckb785");
                let bhckb786 = f("bhckb786");
                let bhckb787 = f("bhckb787");
                let bhckb788 = f("bhckb788");
                let bhckb789 = f("bhckb789");
                let bhckb804 = f("bhckb804");
                let bhckb805 = f("bhckb805");
                let bhckb808 = f("bhckb808");
                let bhckb809 = f("bhckb809");
                let bhckb982 = f("bhckb982");
                let bhckb989 = f("bhckb989");
                let bhckb995 = f("bhckb995");
                let bhckb997 = f("bhckb997");
                let bhckc015 = f("bhckc015");
                let bhckc018 = f("bhckc018");
                let bhckc026 = f("bhckc026");
                let bhckc027 = f("bhckc027");
                let bhckc217 = f("bhckc217");
                let bhckc218 = f("bhckc218");
                let bhckc227 = f("bhckc227");
                let bhckc242 = f("bhckc242");
                let bhckc244 = f("bhckc244");
                let bhckc245 = f("bhckc245");
                let bhckc247 = f("bhckc247");
                let bhckc248 = f("bhckc248");
                let bhckc249 = f("bhckc249");
                let bhckc388 = f("bhckc388");
                let bhckc389 = f("bhckc389");
                let bhckc391 = f("bhckc391");
                let bhckc393 = f("bhckc393");
                let bhckc394 = f("bhckc394");
                let bhckc395 = f("bhckc395");
                let bhckc396 = f("bhckc396");
                let bhckc397 = f("bhckc397");
                let bhckc398 = f("bhckc398");
                let bhckc399 = f("bhckc399");
                let bhckc400 = f("bhckc400");
                let bhckc401 = f("bhckc401");
                let bhckc402 = f("bhckc402");
                let bhckc403 = f("bhckc403");
                let bhckc404 = f("bhckc404");
                let bhckc405 = f("bhckc405");
                let bhckc406 = f("bhckc406");
                let bhckc407 = f("bhckc407");
                let bhckc408 = f("bhckc408");
                let bhckc409 = f("bhckc409");
                let bhckc502 = f("bhckc502");
                let bhckc699 = f("bhckc699");
                let bhckc779 = f("bhckc779");
                let bhckc780 = f("bhckc780");
                let bhckc866 = f("bhckc866");
                let bhckc867 = f("bhckc867");
                let bhckc868 = f("bhckc868");
                let bhckd957 = f("bhckd957");
                let bhckd961 = f("bhckd961");
                let bhckd966 = f("bhckd966");
                let bhckd976 = f("bhckd976");
                let bhckd977 = f("bhckd977");
                let bhckd978 = f("bhckd978");
                let bhckd979 = f("bhckd979");
                let bhckd980 = f("bhckd980");
                let bhckd981 = f("bhckd981");
                let bhckd987 = f("bhckd987");
                let bhckd988 = f("bhckd988");
                let bhckd989 = f("bhckd989");
                let bhckd990 = f("bhckd990");
                let bhckd997 = f("bhckd997");
                let bhckd998 = f("bhckd998");
                let bhckd999 = f("bhckd999");
                let bhckf064 = f("bhckf064");
                let bhckf065 = f("bhckf065");
                let bhckf066 = f("bhckf066");
                let bhckf067 = f("bhckf067");
                let bhckf068 = f("bhckf068");
                let bhckf069 = f("bhckf069");
                let bhckf186 = f("bhckf186");
                let bhckf187 = f("bhckf187");
                let bhckf188 = f("bhckf188");
                let bhckf230 = f("bhckf230");
                let bhckf231 = f("bhckf231");
                let bhckf232 = f("bhckf232");
                let bhckf240 = f("bhckf240");
                let bhckf243 = f("bhckf243");
                let bhckf246 = f("bhckf246");
                let bhckf249 = f("bhckf249");
                let bhckf252 = f("bhckf252");
                let bhckf255 = f("bhckf255");
                let bhckf258 = f("bhckf258");
                let bhckf261 = f("bhckf261");
                let bhckf559 = f("bhckf559");
                let bhckf597 = f("bhckf597");
                let bhckf598 = f("bhckf598");
                let bhckf599 = f("bhckf599");
                let bhckf600 = f("bhckf600");
                let bhckf601 = f("bhckf601");
                let bhckf609 = f("bhckf609");
                let bhckf610 = f("bhckf610");
                let bhckf614 = f("bhckf614");
                let bhckf615 = f("bhckf615");
                let bhckf616 = f("bhckf616");
                let bhckf617 = f("bhckf617");
                let bhckf618 = f("bhckf618");
                let bhckf624 = f("bhckf624");
                let bhckf632 = f("bhckf632");
                let bhckf633 = f("bhckf633");
                let bhckf634 = f("bhckf634");
                let bhckf635 = f("bhckf635");
                let bhckf636 = f("bhckf636");
                let bhckf641 = f("bhckf641");
                let bhckf642 = f("bhckf642");
                let bhckf643 = f("bhckf643");
                let bhckf644 = f("bhckf644");
                let bhckf645 = f("bhckf645");
                let bhckf646 = f("bhckf646");
                let bhckf647 = f("bhckf647");
                let bhckf648 = f("bhckf648");
                let bhckf649 = f("bhckf649");
                let bhckf650 = f("bhckf650");
                let bhckf651 = f("bhckf651");
                let bhckf652 = f("bhckf652");
                let bhckf653 = f("bhckf653");
                let bhckf654 = f("bhckf654");
                let bhckf656 = f("bhckf656");
                let bhckf657 = f("bhckf657");
                let bhckf659 = f("bhckf659");
                let bhckf660 = f("bhckf660");
                let bhckf667 = f("bhckf667");
                let bhckf668 = f("bhckf668");
                let bhckf669 = f("bhckf669");
                let bhckf699 = f("bhckf699");
                let bhckf790 = f("bhckf790");
                let bhckf837 = f("bhckf837");
                let bhckf838 = f("bhckf838");
                let bhckf842 = b("bhckf842");
                let bhckft04 = f("bhckft04");
                let bhckft05 = f("bhckft05");
                let bhckg105 = f("bhckg105");
                let bhckg214 = f("bhckg214");
                let bhckg215 = f("bhckg215");
                let bhckg216 = f("bhckg216");
                let bhckg217 = f("bhckg217");
                let bhckg219 = f("bhckg219");
                let bhckg220 = f("bhckg220");
                let bhckg222 = f("bhckg222");
                let bhckg299 = f("bhckg299");
                let bhckg332 = f("bhckg332");
                let bhckg333 = f("bhckg333");
                let bhckg334 = f("bhckg334");
                let bhckg335 = f("bhckg335");
                let bhckg348 = f("bhckg348");
                let bhckg349 = f("bhckg349");
                let bhckg350 = f("bhckg350");
                let bhckg351 = f("bhckg351");
                let bhckg352 = f("bhckg352");
                let bhckg353 = f("bhckg353");
                let bhckg354 = f("bhckg354");
                let bhckg355 = f("bhckg355");
                let bhckg356 = f("bhckg356");
                let bhckg357 = f("bhckg357");
                let bhckg358 = f("bhckg358");
                let bhckg359 = f("bhckg359");
                let bhckg360 = f("bhckg360");
                let bhckg361 = f("bhckg361");
                let bhckg362 = f("bhckg362");
                let bhckg363 = f("bhckg363");
                let bhckg364 = f("bhckg364");
                let bhckg365 = f("bhckg365");
                let bhckg366 = f("bhckg366");
                let bhckg367 = f("bhckg367");
                let bhckg368 = f("bhckg368");
                let bhckg369 = f("bhckg369");
                let bhckg370 = f("bhckg370");
                let bhckg371 = f("bhckg371");
                let bhckg372 = f("bhckg372");
                let bhckg373 = f("bhckg373");
                let bhckg374 = f("bhckg374");
                let bhckg375 = f("bhckg375");
                let bhckg378 = f("bhckg378");
                let bhckg379 = f("bhckg379");
                let bhckg380 = f("bhckg380");
                let bhckg381 = f("bhckg381");
                let bhckg382 = f("bhckg382");
                let bhckg383 = f("bhckg383");
                let bhckg384 = f("bhckg384");
                let bhckg385 = f("bhckg385");
                let bhckg386 = f("bhckg386");
                let bhckg387 = f("bhckg387");
                let bhckg388 = f("bhckg388");
                let bhckg418 = f("bhckg418");
                let bhckg419 = f("bhckg419");
                let bhckg420 = f("bhckg420");
                let bhckg421 = f("bhckg421");
                let bhckg422 = f("bhckg422");
                let bhckg423 = f("bhckg423");
                let bhckg424 = f("bhckg424");
                let bhckg425 = f("bhckg425");
                let bhckg426 = f("bhckg426");
                let bhckg427 = f("bhckg427");
                let bhckg428 = f("bhckg428");
                let bhckg429 = f("bhckg429");
                let bhckg430 = f("bhckg430");
                let bhckg431 = f("bhckg431");
                let bhckg432 = f("bhckg432");
                let bhckg433 = f("bhckg433");
                let bhckg434 = f("bhckg434");
                let bhckg435 = f("bhckg435");
                let bhckg436 = f("bhckg436");
                let bhckg437 = f("bhckg437");
                let bhckg438 = f("bhckg438");
                let bhckg439 = f("bhckg439");
                let bhckg440 = f("bhckg440");
                let bhckg441 = f("bhckg441");
                let bhckg442 = f("bhckg442");
                let bhckg443 = f("bhckg443");
                let bhckg444 = f("bhckg444");
                let bhckg445 = f("bhckg445");
                let bhckg446 = f("bhckg446");
                let bhckg447 = f("bhckg447");
                let bhckg448 = f("bhckg448");
                let bhckg449 = f("bhckg449");
                let bhckg450 = f("bhckg450");
                let bhckg451 = f("bhckg451");
                let bhckg452 = f("bhckg452");
                let bhckg453 = f("bhckg453");
                let bhckg454 = f("bhckg454");
                let bhckg455 = f("bhckg455");
                let bhckg456 = f("bhckg456");
                let bhckg457 = f("bhckg457");
                let bhckg458 = f("bhckg458");
                let bhckg459 = f("bhckg459");
                let bhckg460 = f("bhckg460");
                let bhckg461 = f("bhckg461");
                let bhckg462 = f("bhckg462");
                let bhckg493 = f("bhckg493");
                let bhckg494 = f("bhckg494");
                let bhckg495 = f("bhckg495");
                let bhckg496 = f("bhckg496");
                let bhckg497 = f("bhckg497");
                let bhckg498 = f("bhckg498");
                let bhckg499 = f("bhckg499");
                let bhckg500 = f("bhckg500");
                let bhckg501 = f("bhckg501");
                let bhckg502 = f("bhckg502");
                let bhckg503 = f("bhckg503");
                let bhckg504 = f("bhckg504");
                let bhckg505 = f("bhckg505");
                let bhckg506 = f("bhckg506");
                let bhckg512 = f("bhckg512");
                let bhckg513 = f("bhckg513");
                let bhckg514 = f("bhckg514");
                let bhckg515 = f("bhckg515");
                let bhckg516 = f("bhckg516");
                let bhckg517 = f("bhckg517");
                let bhckg518 = f("bhckg518");
                let bhckg519 = f("bhckg519");
                let bhckg520 = f("bhckg520");
                let bhckg526 = f("bhckg526");
                let bhckg527 = f("bhckg527");
                let bhckg528 = f("bhckg528");
                let bhckg529 = f("bhckg529");
                let bhckg530 = f("bhckg530");
                let bhckg531 = f("bhckg531");
                let bhckg532 = f("bhckg532");
                let bhckg533 = f("bhckg533");
                let bhckg534 = f("bhckg534");
                let bhckg535 = f("bhckg535");
                let bhckg551 = f("bhckg551");
                let bhckg552 = f("bhckg552");
                let bhckg553 = f("bhckg553");
                let bhckg554 = f("bhckg554");
                let bhckg555 = f("bhckg555");
                let bhckg556 = f("bhckg556");
                let bhckg557 = f("bhckg557");
                let bhckg558 = f("bhckg558");
                let bhckg559 = f("bhckg559");
                let bhckg560 = f("bhckg560");
                let bhckg576 = f("bhckg576");
                let bhckg577 = f("bhckg577");
                let bhckg578 = f("bhckg578");
                let bhckg579 = f("bhckg579");
                let bhckg580 = f("bhckg580");
                let bhckg581 = f("bhckg581");
                let bhckg582 = f("bhckg582");
                let bhckg583 = f("bhckg583");
                let bhckg584 = f("bhckg584");
                let bhckg585 = f("bhckg585");
                let bhckg591 = f("bhckg591");
                let bhckg603 = f("bhckg603");
                let bhckg604 = f("bhckg604");
                let bhckg605 = f("bhckg605");
                let bhckg612 = f("bhckg612");
                let bhckg613 = f("bhckg613");
                let bhckg614 = f("bhckg614");
                let bhckg615 = f("bhckg615");
                let bhckg616 = f("bhckg616");
                let bhckg617 = f("bhckg617");
                let bhckg624 = f("bhckg624");
                let bhckg625 = f("bhckg625");
                let bhckg626 = f("bhckg626");
                let bhckg627 = f("bhckg627");
                let bhckg628 = f("bhckg628");
                let bhckg629 = f("bhckg629");
                let bhckg630 = f("bhckg630");
                let bhckg631 = f("bhckg631");
                let bhckg632 = f("bhckg632");
                let bhckg633 = f("bhckg633");
                let bhckg634 = f("bhckg634");
                let bhckg635 = f("bhckg635");
                let bhckg636 = f("bhckg636");
                let bhckg637 = f("bhckg637");
                let bhckg641 = f("bhckg641");
                let bhckg651 = f("bhckg651");
                let bhckg652 = f("bhckg652");
                let bhckh171 = f("bhckh171");
                let bhckh191 = f("bhckh191");
                let bhckh289 = f("bhckh289");
                let bhckh290 = f("bhckh290");
                let bhckh291 = f("bhckh291");
                let bhckh292 = f("bhckh292");
                let bhckh300 = f("bhckh300");
                let bhckh301 = f("bhckh301");
                let bhckh302 = f("bhckh302");
                let bhckh303 = f("bhckh303");
                let bhckh304 = f("bhckh304");
                let bhckh307 = f("bhckh307");
                let bhckh308 = f("bhckh308");
                let bhckh309 = f("bhckh309");
                let bhckh310 = f("bhckh310");
                let bhckhj74 = f("bhckhj74");
                let bhckhj75 = f("bhckhj75");
                let bhckhj76 = f("bhckhj76");
                let bhckhj77 = f("bhckhj77");
                let bhckhj86 = f("bhckhj86");
                let bhckhj87 = f("bhckhj87");
                let bhckhj90 = f("bhckhj90");
                let bhckhj91 = f("bhckhj91");
                let bhckhj96 = f("bhckhj96");
                let bhckhj97 = f("bhckhj97");
                let bhckhj98 = f("bhckhj98");
                let bhckhj99 = f("bhckhj99");
                let bhckhk00 = f("bhckhk00");
                let bhckhk01 = f("bhckhk01");
                let bhckhk25 = f("bhckhk25");
                let bhckhk26 = f("bhckhk26");
                let bhckhk27 = f("bhckhk27");
                let bhckhk28 = f("bhckhk28");
                let bhckht50 = f("bhckht50");
                let bhckht51 = f("bhckht51");
                let bhckht52 = f("bhckht52");
                let bhckht53 = f("bhckht53");
                let bhckht66 = f("bhckht66");
                let bhckht67 = f("bhckht67");
                let bhckht68 = f("bhckht68");
                let bhckht70 = f("bhckht70");
                let bhckht81 = f("bhckht81");
                let bhckht82 = f("bhckht82");
                let bhckht86 = f("bhckht86");
                let bhckhu16 = f("bhckhu16");
                let bhckhu17 = f("bhckhu17");
                let bhckhu18 = f("bhckhu18");
                let bhckj319 = f("bhckj319");
                let bhckj321 = f("bhckj321");
                let bhckj457 = f("bhckj457");
                let bhckj458 = f("bhckj458");
                let bhckj459 = f("bhckj459");
                let bhckjf77 = f("bhckjf77");
                let bhckjf78 = f("bhckjf78");
                let bhckjh89 = f("bhckjh89");
                let bhckjh90 = f("bhckjh90");
                let bhckjh95 = f("bhckjh95");
                let bhckjh96 = f("bhckjh96");
                let bhckjj02 = f("bhckjj02");
                let bhckjj33 = f("bhckjj33");
                let bhckk042 = f("bhckk042");
                let bhckk043 = f("bhckk043");
                let bhckk044 = f("bhckk044");
                let bhckk102 = f("bhckk102");
                let bhckk103 = f("bhckk103");
                let bhckk104 = f("bhckk104");
                let bhckk133 = f("bhckk133");
                let bhckk141 = f("bhckk141");
                let bhckk195 = f("bhckk195");
                let bhckk197 = f("bhckk197");
                let bhckk198 = f("bhckk198");
                let bhckk199 = f("bhckk199");
                let bhckk200 = f("bhckk200");
                let bhckk206 = f("bhckk206");
                let bhckk209 = f("bhckk209");
                let bhckk210 = f("bhckk210");
                let bhckk211 = f("bhckk211");
                let bhckkx48 = f("bhckkx48");
                let bhckkx49 = f("bhckkx49");
                let bhckkx56 = f("bhckkx56");
                let bhckkx59 = f("bhckkx59");
                let bhckkx66 = f("bhckkx66");
                let bhckkx67 = f("bhckkx67");
                let bhckkx68 = f("bhckkx68");
                let bhckl183 = f("bhckl183");
                let bhckl184 = f("bhckl184");
                let bhckl185 = f("bhckl185");
                let bhckl186 = f("bhckl186");
                let bhckl187 = f("bhckl187");
                let bhckl188 = f("bhckl188");
                let bhckl191 = b("bhckl191");
                let bhckl192 = b("bhckl192");
                let bhckle75 = f("bhckle75");
                let bhcklg25 = b("bhcklg25");
                let bhcklg27 = f("bhcklg27");
                let bhcklg28 = f("bhcklg28");
                let bhckll57 = f("bhckll57");
                let bhckm288 = f("bhckm288");
                let bhckm708 = f("bhckm708");
                let bhckm709 = f("bhckm709");
                let bhckm710 = f("bhckm710");
                let bhckm711 = f("bhckm711");
                let bhckm712 = f("bhckm712");
                let bhckm713 = f("bhckm713");
                let bhckm714 = f("bhckm714");
                let bhckm715 = f("bhckm715");
                let bhckm716 = f("bhckm716");
                let bhckm717 = f("bhckm717");
                let bhckm719 = f("bhckm719");
                let bhckm720 = f("bhckm720");
                let bhckm721 = f("bhckm721");
                let bhckm722 = f("bhckm722");
                let bhckm723 = f("bhckm723");
                let bhckm724 = f("bhckm724");
                let bhckm725 = f("bhckm725");
                let bhckm726 = f("bhckm726");
                let bhckm745 = f("bhckm745");
                let bhckm746 = f("bhckm746");
                let bhckm747 = f("bhckm747");
                let bhckm748 = f("bhckm748");
                let bhckm749 = f("bhckm749");
                let bhckm750 = f("bhckm750");
                let bhckm751 = f("bhckm751");
                let bhckmg93 = f("bhckmg93");
                let bhckmg95 = f("bhckmg95");
                let bhcks413 = f("bhcks413");
                let bhcks419 = f("bhcks419");
                let bhcks423 = f("bhcks423");
                let bhcks431 = f("bhcks431");
                let bhcks439 = f("bhcks439");
                let bhcks445 = f("bhcks445");
                let bhcks449 = f("bhcks449");
                let bhcks457 = f("bhcks457");
                let bhcks466 = f("bhcks466");
                let bhcks467 = f("bhcks467");
                let bhcks475 = f("bhcks475");
                let bhcks480 = f("bhcks480");
                let bhcks485 = f("bhcks485");
                let bhcks490 = f("bhcks490");
                let bhcks495 = f("bhcks495");
                let bhcks500 = f("bhcks500");
                let bhcks503 = f("bhcks503");
                let bhcks504 = f("bhcks504");
                let bhcks505 = f("bhcks505");
                let bhcks506 = f("bhcks506");
                let bhcks507 = f("bhcks507");
                let bhcks510 = f("bhcks510");
                let bhcks512 = f("bhcks512");
                let bhcks514 = f("bhcks514");
                let bhcks515 = f("bhcks515");
                let bhcks516 = f("bhcks516");
                let bhcks517 = f("bhcks517");
                let bhcks518 = f("bhcks518");
                let bhcks519 = f("bhcks519");
                let bhcks520 = f("bhcks520");
                let bhcks521 = f("bhcks521");
                let bhcks522 = f("bhcks522");
                let bhcks523 = f("bhcks523");
                let bhcks525 = f("bhcks525");
                let bhcks526 = f("bhcks526");
                let bhcks527 = f("bhcks527");
                let bhcks528 = f("bhcks528");
                let bhcks529 = f("bhcks529");
                let bhcks530 = f("bhcks530");
                let bhcks531 = f("bhcks531");
                let bhcks539 = f("bhcks539");
                let bhcks540 = f("bhcks540");
                let bhcks541 = f("bhcks541");
                let bhcks542 = f("bhcks542");
                let bhcks543 = f("bhcks543");
                let bhcks544 = f("bhcks544");
                let bhcks545 = f("bhcks545");
                let bhcks546 = f("bhcks546");
                let bhcks547 = f("bhcks547");
                let bhcks548 = f("bhcks548");
                let bhcks558 = f("bhcks558");
                let bhcks559 = f("bhcks559");
                let bhcks560 = f("bhcks560");
                let bhcks561 = f("bhcks561");
                let bhcks562 = f("bhcks562");
                let bhcks563 = f("bhcks563");
                let bhcks564 = f("bhcks564");
                let bhcks565 = f("bhcks565");
                let bhcks566 = f("bhcks566");
                let bhcks567 = f("bhcks567");
                let bhcks568 = f("bhcks568");
                let bhcks569 = f("bhcks569");
                let bhcks570 = f("bhcks570");
                let bhcks571 = f("bhcks571");
                let bhcks572 = f("bhcks572");
                let bhcks573 = f("bhcks573");
                let bhcks574 = f("bhcks574");
                let bhcks575 = f("bhcks575");
                let bhcks576 = f("bhcks576");
                let bhcks577 = f("bhcks577");
                let bhcks578 = f("bhcks578");
                let bhcks579 = f("bhcks579");
                let bhcks580 = f("bhcks580");
                let bhcks581 = f("bhcks581");
                let bhcks624 = f("bhcks624");
                let rssd9001 = f("rssd9001");
                let rssd9017 = s("rssd9017");
                let rssd9999 = f("rssd9999");
                let wrdsdownloaddate = d("wrdsdownloaddate");

                for row_i in 0..batch.num_rows() {
                    let mut vals: Vec<AnyValue<'static>> = Vec::with_capacity(811);
                    vals.push(if bhck0383.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0383.value(row_i))
                    });
                    vals.push(if bhck0384.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0384.value(row_i))
                    });
                    vals.push(if bhck0387.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0387.value(row_i))
                    });
                    vals.push(if bhck0416.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0416.value(row_i))
                    });
                    vals.push(if bhck0535.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck0535.value(row_i))
                    });
                    vals.push(if bhck1395.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1395.value(row_i))
                    });
                    vals.push(if bhck1403.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1403.value(row_i))
                    });
                    vals.push(if bhck1406.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1406.value(row_i))
                    });
                    vals.push(if bhck1407.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1407.value(row_i))
                    });
                    vals.push(if bhck1658.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1658.value(row_i))
                    });
                    vals.push(if bhck1659.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1659.value(row_i))
                    });
                    vals.push(if bhck1661.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1661.value(row_i))
                    });
                    vals.push(if bhck1771.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1771.value(row_i))
                    });
                    vals.push(if bhck1772.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1772.value(row_i))
                    });
                    vals.push(if bhck1914.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck1914.value(row_i))
                    });
                    vals.push(if bhck2033.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2033.value(row_i))
                    });
                    vals.push(if bhck2079.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2079.value(row_i))
                    });
                    vals.push(if bhck2122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2122.value(row_i))
                    });
                    vals.push(if bhck2123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2123.value(row_i))
                    });
                    vals.push(if bhck2125.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2125.value(row_i))
                    });
                    vals.push(if bhck2145.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2145.value(row_i))
                    });
                    vals.push(if bhck2170.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2170.value(row_i))
                    });
                    vals.push(if bhck2221.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2221.value(row_i))
                    });
                    vals.push(if bhck2419.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2419.value(row_i))
                    });
                    vals.push(if bhck2432.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2432.value(row_i))
                    });
                    vals.push(if bhck2635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2635.value(row_i))
                    });
                    vals.push(if bhck2744.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2744.value(row_i))
                    });
                    vals.push(if bhck2948.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck2948.value(row_i))
                    });
                    vals.push(if bhck3196.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3196.value(row_i))
                    });
                    vals.push(if bhck3210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3210.value(row_i))
                    });
                    vals.push(if bhck3240.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3240.value(row_i))
                    });
                    vals.push(if bhck3247.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3247.value(row_i))
                    });
                    vals.push(if bhck3283.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3283.value(row_i))
                    });
                    vals.push(if bhck3290.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3290.value(row_i))
                    });
                    vals.push(if bhck3293.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3293.value(row_i))
                    });
                    vals.push(if bhck3300.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3300.value(row_i))
                    });
                    vals.push(if bhck3353.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3353.value(row_i))
                    });
                    vals.push(if bhck3365.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3365.value(row_i))
                    });
                    vals.push(if bhck3368.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3368.value(row_i))
                    });
                    vals.push(if bhck3376.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3376.value(row_i))
                    });
                    vals.push(if bhck3377.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3377.value(row_i))
                    });
                    vals.push(if bhck3378.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3378.value(row_i))
                    });
                    vals.push(if bhck3401.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3401.value(row_i))
                    });
                    vals.push(if bhck3402.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3402.value(row_i))
                    });
                    vals.push(if bhck3404.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3404.value(row_i))
                    });
                    vals.push(if bhck3408.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3408.value(row_i))
                    });
                    vals.push(if bhck3428.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3428.value(row_i))
                    });
                    vals.push(if bhck3429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3429.value(row_i))
                    });
                    vals.push(if bhck3432.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3432.value(row_i))
                    });
                    vals.push(if bhck3433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3433.value(row_i))
                    });
                    vals.push(if bhck3459.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3459.value(row_i))
                    });
                    vals.push(if bhck3515.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3515.value(row_i))
                    });
                    vals.push(if bhck3516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3516.value(row_i))
                    });
                    vals.push(if bhck3517.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3517.value(row_i))
                    });
                    vals.push(if bhck3519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3519.value(row_i))
                    });
                    vals.push(if bhck3521.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3521.value(row_i))
                    });
                    vals.push(if bhck3531.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3531.value(row_i))
                    });
                    vals.push(if bhck3532.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3532.value(row_i))
                    });
                    vals.push(if bhck3533.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3533.value(row_i))
                    });
                    vals.push(if bhck3534.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3534.value(row_i))
                    });
                    vals.push(if bhck3535.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3535.value(row_i))
                    });
                    vals.push(if bhck3536.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3536.value(row_i))
                    });
                    vals.push(if bhck3537.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3537.value(row_i))
                    });
                    vals.push(if bhck3542.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3542.value(row_i))
                    });
                    vals.push(if bhck3543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3543.value(row_i))
                    });
                    vals.push(if bhck3545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3545.value(row_i))
                    });
                    vals.push(if bhck3547.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3547.value(row_i))
                    });
                    vals.push(if bhck3548.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3548.value(row_i))
                    });
                    vals.push(if bhck3573.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3573.value(row_i))
                    });
                    vals.push(if bhck3575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3575.value(row_i))
                    });
                    vals.push(if bhck3577.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3577.value(row_i))
                    });
                    vals.push(if bhck3579.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3579.value(row_i))
                    });
                    vals.push(if bhck3583.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3583.value(row_i))
                    });
                    vals.push(if bhck3585.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3585.value(row_i))
                    });
                    vals.push(if bhck3589.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3589.value(row_i))
                    });
                    vals.push(if bhck3591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3591.value(row_i))
                    });
                    vals.push(if bhck3792.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3792.value(row_i))
                    });
                    vals.push(if bhck3814.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3814.value(row_i))
                    });
                    vals.push(if bhck3815.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3815.value(row_i))
                    });
                    vals.push(if bhck3817.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3817.value(row_i))
                    });
                    vals.push(if bhck3818.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck3818.value(row_i))
                    });
                    vals.push(if bhck4062.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4062.value(row_i))
                    });
                    vals.push(if bhck4073.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4073.value(row_i))
                    });
                    vals.push(if bhck4079.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4079.value(row_i))
                    });
                    vals.push(if bhck4093.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4093.value(row_i))
                    });
                    vals.push(if bhck4107.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4107.value(row_i))
                    });
                    vals.push(if bhck4135.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4135.value(row_i))
                    });
                    vals.push(if bhck4230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4230.value(row_i))
                    });
                    vals.push(if bhck4243.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4243.value(row_i))
                    });
                    vals.push(if bhck4307.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4307.value(row_i))
                    });
                    vals.push(if bhck4483.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4483.value(row_i))
                    });
                    vals.push(if bhck4505.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4505.value(row_i))
                    });
                    vals.push(if bhck4605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4605.value(row_i))
                    });
                    vals.push(if bhck4617.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4617.value(row_i))
                    });
                    vals.push(if bhck4618.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4618.value(row_i))
                    });
                    vals.push(if bhck4627.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4627.value(row_i))
                    });
                    vals.push(if bhck4628.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4628.value(row_i))
                    });
                    vals.push(if bhck4661.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4661.value(row_i))
                    });
                    vals.push(if bhck4662.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4662.value(row_i))
                    });
                    vals.push(if bhck4663.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4663.value(row_i))
                    });
                    vals.push(if bhck4664.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4664.value(row_i))
                    });
                    vals.push(if bhck4665.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4665.value(row_i))
                    });
                    vals.push(if bhck4666.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4666.value(row_i))
                    });
                    vals.push(if bhck4667.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4667.value(row_i))
                    });
                    vals.push(if bhck4668.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4668.value(row_i))
                    });
                    vals.push(if bhck4669.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4669.value(row_i))
                    });
                    vals.push(if bhck4782.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4782.value(row_i))
                    });
                    vals.push(if bhck4783.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck4783.value(row_i))
                    });
                    vals.push(if bhck5306.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5306.value(row_i))
                    });
                    vals.push(if bhck5311.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5311.value(row_i))
                    });
                    vals.push(if bhck5352.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5352.value(row_i))
                    });
                    vals.push(if bhck5353.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5353.value(row_i))
                    });
                    vals.push(if bhck5357.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5357.value(row_i))
                    });
                    vals.push(if bhck5358.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5358.value(row_i))
                    });
                    vals.push(if bhck5376.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5376.value(row_i))
                    });
                    vals.push(if bhck5396.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5396.value(row_i))
                    });
                    vals.push(if bhck5410.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5410.value(row_i))
                    });
                    vals.push(if bhck5412.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5412.value(row_i))
                    });
                    vals.push(if bhck5414.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5414.value(row_i))
                    });
                    vals.push(if bhck5479.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5479.value(row_i))
                    });
                    vals.push(if bhck5483.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5483.value(row_i))
                    });
                    vals.push(if bhck5484.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5484.value(row_i))
                    });
                    vals.push(if bhck5500.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5500.value(row_i))
                    });
                    vals.push(if bhck5501.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5501.value(row_i))
                    });
                    vals.push(if bhck5502.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5502.value(row_i))
                    });
                    vals.push(if bhck5503.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5503.value(row_i))
                    });
                    vals.push(if bhck5504.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5504.value(row_i))
                    });
                    vals.push(if bhck5505.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5505.value(row_i))
                    });
                    vals.push(if bhck5523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5523.value(row_i))
                    });
                    vals.push(if bhck5524.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5524.value(row_i))
                    });
                    vals.push(if bhck5525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5525.value(row_i))
                    });
                    vals.push(if bhck5526.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5526.value(row_i))
                    });
                    vals.push(if bhck5990.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck5990.value(row_i))
                    });
                    vals.push(if bhck6562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6562.value(row_i))
                    });
                    vals.push(if bhck6568.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6568.value(row_i))
                    });
                    vals.push(if bhck6570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6570.value(row_i))
                    });
                    vals.push(if bhck6577.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck6577.value(row_i))
                    });
                    vals.push(if bhck6996.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck6996.value(row_i))
                    });
                    vals.push(if bhck6997.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck6997.value(row_i))
                    });
                    vals.push(if bhck7204.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck7204.value(row_i))
                    });
                    vals.push(if bhck7205.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck7205.value(row_i))
                    });
                    vals.push(if bhck7206.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck7206.value(row_i))
                    });
                    vals.push(if bhck8274.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8274.value(row_i))
                    });
                    vals.push(if bhck8275.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8275.value(row_i))
                    });
                    vals.push(if bhck8551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8551.value(row_i))
                    });
                    vals.push(if bhck8552.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8552.value(row_i))
                    });
                    vals.push(if bhck8553.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8553.value(row_i))
                    });
                    vals.push(if bhck8554.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8554.value(row_i))
                    });
                    vals.push(if bhck8555.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8555.value(row_i))
                    });
                    vals.push(if bhck8556.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8556.value(row_i))
                    });
                    vals.push(if bhck8701.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8701.value(row_i))
                    });
                    vals.push(if bhck8702.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8702.value(row_i))
                    });
                    vals.push(if bhck8703.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8703.value(row_i))
                    });
                    vals.push(if bhck8704.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8704.value(row_i))
                    });
                    vals.push(if bhck8705.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8705.value(row_i))
                    });
                    vals.push(if bhck8706.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8706.value(row_i))
                    });
                    vals.push(if bhck8707.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8707.value(row_i))
                    });
                    vals.push(if bhck8708.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8708.value(row_i))
                    });
                    vals.push(if bhck8709.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8709.value(row_i))
                    });
                    vals.push(if bhck8710.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8710.value(row_i))
                    });
                    vals.push(if bhck8711.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8711.value(row_i))
                    });
                    vals.push(if bhck8712.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8712.value(row_i))
                    });
                    vals.push(if bhck8713.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8713.value(row_i))
                    });
                    vals.push(if bhck8714.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8714.value(row_i))
                    });
                    vals.push(if bhck8715.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8715.value(row_i))
                    });
                    vals.push(if bhck8716.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8716.value(row_i))
                    });
                    vals.push(if bhck8717.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck8717.value(row_i))
                    });
                    vals.push(if bhck8718.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck8718.value(row_i))
                    });
                    vals.push(if bhck8723.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8723.value(row_i))
                    });
                    vals.push(if bhck8724.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8724.value(row_i))
                    });
                    vals.push(if bhck8725.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8725.value(row_i))
                    });
                    vals.push(if bhck8726.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8726.value(row_i))
                    });
                    vals.push(if bhck8727.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8727.value(row_i))
                    });
                    vals.push(if bhck8728.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8728.value(row_i))
                    });
                    vals.push(if bhck8729.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8729.value(row_i))
                    });
                    vals.push(if bhck8730.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8730.value(row_i))
                    });
                    vals.push(if bhck8731.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8731.value(row_i))
                    });
                    vals.push(if bhck8732.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8732.value(row_i))
                    });
                    vals.push(if bhck8765.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8765.value(row_i))
                    });
                    vals.push(if bhck8768.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck8768.value(row_i))
                    });
                    vals.push(if bhck8784.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck8784.value(row_i))
                    });
                    vals.push(if bhck8834.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck8834.value(row_i))
                    });
                    vals.push(if bhck8836.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck8836.value(row_i))
                    });
                    vals.push(if bhck8838.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhck8838.value(row_i))
                    });
                    vals.push(if bhck9191.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck9191.value(row_i))
                    });
                    vals.push(if bhck9802.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhck9802.value(row_i))
                    });
                    vals.push(if bhcka102.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka102.value(row_i))
                    });
                    vals.push(if bhcka120.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcka120.value(row_i))
                    });
                    vals.push(if bhcka121.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcka121.value(row_i))
                    });
                    vals.push(if bhcka122.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcka122.value(row_i))
                    });
                    vals.push(if bhcka123.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcka123.value(row_i))
                    });
                    vals.push(if bhcka124.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcka124.value(row_i))
                    });
                    vals.push(if bhcka126.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka126.value(row_i))
                    });
                    vals.push(if bhcka127.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka127.value(row_i))
                    });
                    vals.push(if bhcka128.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcka128.value(row_i))
                    });
                    vals.push(if bhcka195.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka195.value(row_i))
                    });
                    vals.push(if bhcka220.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka220.value(row_i))
                    });
                    vals.push(if bhcka223.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka223.value(row_i))
                    });
                    vals.push(if bhcka249.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka249.value(row_i))
                    });
                    vals.push(if bhcka288.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka288.value(row_i))
                    });
                    vals.push(if bhcka591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcka591.value(row_i))
                    });
                    vals.push(if bhckb027.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb027.value(row_i))
                    });
                    vals.push(if bhckb028.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb028.value(row_i))
                    });
                    vals.push(if bhckb031.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb031.value(row_i))
                    });
                    vals.push(if bhckb033.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb033.value(row_i))
                    });
                    vals.push(if bhckb034.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb034.value(row_i))
                    });
                    vals.push(if bhckb037.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb037.value(row_i))
                    });
                    vals.push(if bhckb038.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb038.value(row_i))
                    });
                    vals.push(if bhckb041.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb041.value(row_i))
                    });
                    vals.push(if bhckb042.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb042.value(row_i))
                    });
                    vals.push(if bhckb043.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb043.value(row_i))
                    });
                    vals.push(if bhckb046.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb046.value(row_i))
                    });
                    vals.push(if bhckb048.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb048.value(row_i))
                    });
                    vals.push(if bhckb049.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb049.value(row_i))
                    });
                    vals.push(if bhckb052.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb052.value(row_i))
                    });
                    vals.push(if bhckb053.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb053.value(row_i))
                    });
                    vals.push(if bhckb056.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb056.value(row_i))
                    });
                    vals.push(if bhckb491.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb491.value(row_i))
                    });
                    vals.push(if bhckb507.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb507.value(row_i))
                    });
                    vals.push(if bhckb513.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb513.value(row_i))
                    });
                    vals.push(if bhckb515.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb515.value(row_i))
                    });
                    vals.push(if bhckb517.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb517.value(row_i))
                    });
                    vals.push(if bhckb541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb541.value(row_i))
                    });
                    vals.push(if bhckb558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb558.value(row_i))
                    });
                    vals.push(if bhckb589.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb589.value(row_i))
                    });
                    vals.push(if bhckb696.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb696.value(row_i))
                    });
                    vals.push(if bhckb697.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb697.value(row_i))
                    });
                    vals.push(if bhckb698.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb698.value(row_i))
                    });
                    vals.push(if bhckb699.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb699.value(row_i))
                    });
                    vals.push(if bhckb700.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb700.value(row_i))
                    });
                    vals.push(if bhckb701.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb701.value(row_i))
                    });
                    vals.push(if bhckb702.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb702.value(row_i))
                    });
                    vals.push(if bhckb703.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb703.value(row_i))
                    });
                    vals.push(if bhckb704.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb704.value(row_i))
                    });
                    vals.push(if bhckb705.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb705.value(row_i))
                    });
                    vals.push(if bhckb706.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb706.value(row_i))
                    });
                    vals.push(if bhckb707.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb707.value(row_i))
                    });
                    vals.push(if bhckb708.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb708.value(row_i))
                    });
                    vals.push(if bhckb709.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb709.value(row_i))
                    });
                    vals.push(if bhckb710.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb710.value(row_i))
                    });
                    vals.push(if bhckb711.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb711.value(row_i))
                    });
                    vals.push(if bhckb712.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb712.value(row_i))
                    });
                    vals.push(if bhckb713.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb713.value(row_i))
                    });
                    vals.push(if bhckb714.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb714.value(row_i))
                    });
                    vals.push(if bhckb715.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb715.value(row_i))
                    });
                    vals.push(if bhckb716.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb716.value(row_i))
                    });
                    vals.push(if bhckb717.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb717.value(row_i))
                    });
                    vals.push(if bhckb718.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb718.value(row_i))
                    });
                    vals.push(if bhckb719.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb719.value(row_i))
                    });
                    vals.push(if bhckb720.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb720.value(row_i))
                    });
                    vals.push(if bhckb721.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb721.value(row_i))
                    });
                    vals.push(if bhckb722.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb722.value(row_i))
                    });
                    vals.push(if bhckb723.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb723.value(row_i))
                    });
                    vals.push(if bhckb724.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb724.value(row_i))
                    });
                    vals.push(if bhckb725.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb725.value(row_i))
                    });
                    vals.push(if bhckb726.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb726.value(row_i))
                    });
                    vals.push(if bhckb727.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb727.value(row_i))
                    });
                    vals.push(if bhckb728.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb728.value(row_i))
                    });
                    vals.push(if bhckb729.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb729.value(row_i))
                    });
                    vals.push(if bhckb730.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb730.value(row_i))
                    });
                    vals.push(if bhckb731.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb731.value(row_i))
                    });
                    vals.push(if bhckb732.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb732.value(row_i))
                    });
                    vals.push(if bhckb733.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb733.value(row_i))
                    });
                    vals.push(if bhckb734.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb734.value(row_i))
                    });
                    vals.push(if bhckb735.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb735.value(row_i))
                    });
                    vals.push(if bhckb736.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb736.value(row_i))
                    });
                    vals.push(if bhckb737.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb737.value(row_i))
                    });
                    vals.push(if bhckb738.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb738.value(row_i))
                    });
                    vals.push(if bhckb739.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb739.value(row_i))
                    });
                    vals.push(if bhckb740.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb740.value(row_i))
                    });
                    vals.push(if bhckb741.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb741.value(row_i))
                    });
                    vals.push(if bhckb742.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb742.value(row_i))
                    });
                    vals.push(if bhckb743.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb743.value(row_i))
                    });
                    vals.push(if bhckb744.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb744.value(row_i))
                    });
                    vals.push(if bhckb745.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb745.value(row_i))
                    });
                    vals.push(if bhckb746.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb746.value(row_i))
                    });
                    vals.push(if bhckb754.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb754.value(row_i))
                    });
                    vals.push(if bhckb755.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb755.value(row_i))
                    });
                    vals.push(if bhckb756.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb756.value(row_i))
                    });
                    vals.push(if bhckb757.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb757.value(row_i))
                    });
                    vals.push(if bhckb758.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb758.value(row_i))
                    });
                    vals.push(if bhckb759.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb759.value(row_i))
                    });
                    vals.push(if bhckb760.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb760.value(row_i))
                    });
                    vals.push(if bhckb764.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb764.value(row_i))
                    });
                    vals.push(if bhckb765.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb765.value(row_i))
                    });
                    vals.push(if bhckb766.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb766.value(row_i))
                    });
                    vals.push(if bhckb767.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb767.value(row_i))
                    });
                    vals.push(if bhckb768.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb768.value(row_i))
                    });
                    vals.push(if bhckb769.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb769.value(row_i))
                    });
                    vals.push(if bhckb773.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb773.value(row_i))
                    });
                    vals.push(if bhckb774.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb774.value(row_i))
                    });
                    vals.push(if bhckb775.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb775.value(row_i))
                    });
                    vals.push(if bhckb783.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb783.value(row_i))
                    });
                    vals.push(if bhckb784.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb784.value(row_i))
                    });
                    vals.push(if bhckb785.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb785.value(row_i))
                    });
                    vals.push(if bhckb786.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb786.value(row_i))
                    });
                    vals.push(if bhckb787.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb787.value(row_i))
                    });
                    vals.push(if bhckb788.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb788.value(row_i))
                    });
                    vals.push(if bhckb789.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb789.value(row_i))
                    });
                    vals.push(if bhckb804.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb804.value(row_i))
                    });
                    vals.push(if bhckb805.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb805.value(row_i))
                    });
                    vals.push(if bhckb808.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb808.value(row_i))
                    });
                    vals.push(if bhckb809.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb809.value(row_i))
                    });
                    vals.push(if bhckb982.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb982.value(row_i))
                    });
                    vals.push(if bhckb989.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb989.value(row_i))
                    });
                    vals.push(if bhckb995.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb995.value(row_i))
                    });
                    vals.push(if bhckb997.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckb997.value(row_i))
                    });
                    vals.push(if bhckc015.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc015.value(row_i))
                    });
                    vals.push(if bhckc018.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc018.value(row_i))
                    });
                    vals.push(if bhckc026.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc026.value(row_i))
                    });
                    vals.push(if bhckc027.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc027.value(row_i))
                    });
                    vals.push(if bhckc217.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc217.value(row_i))
                    });
                    vals.push(if bhckc218.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc218.value(row_i))
                    });
                    vals.push(if bhckc227.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc227.value(row_i))
                    });
                    vals.push(if bhckc242.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc242.value(row_i))
                    });
                    vals.push(if bhckc244.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc244.value(row_i))
                    });
                    vals.push(if bhckc245.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc245.value(row_i))
                    });
                    vals.push(if bhckc247.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc247.value(row_i))
                    });
                    vals.push(if bhckc248.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc248.value(row_i))
                    });
                    vals.push(if bhckc249.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc249.value(row_i))
                    });
                    vals.push(if bhckc388.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc388.value(row_i))
                    });
                    vals.push(if bhckc389.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc389.value(row_i))
                    });
                    vals.push(if bhckc391.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc391.value(row_i))
                    });
                    vals.push(if bhckc393.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc393.value(row_i))
                    });
                    vals.push(if bhckc394.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc394.value(row_i))
                    });
                    vals.push(if bhckc395.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc395.value(row_i))
                    });
                    vals.push(if bhckc396.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc396.value(row_i))
                    });
                    vals.push(if bhckc397.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc397.value(row_i))
                    });
                    vals.push(if bhckc398.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc398.value(row_i))
                    });
                    vals.push(if bhckc399.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc399.value(row_i))
                    });
                    vals.push(if bhckc400.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc400.value(row_i))
                    });
                    vals.push(if bhckc401.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc401.value(row_i))
                    });
                    vals.push(if bhckc402.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc402.value(row_i))
                    });
                    vals.push(if bhckc403.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc403.value(row_i))
                    });
                    vals.push(if bhckc404.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc404.value(row_i))
                    });
                    vals.push(if bhckc405.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc405.value(row_i))
                    });
                    vals.push(if bhckc406.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc406.value(row_i))
                    });
                    vals.push(if bhckc407.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc407.value(row_i))
                    });
                    vals.push(if bhckc408.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc408.value(row_i))
                    });
                    vals.push(if bhckc409.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc409.value(row_i))
                    });
                    vals.push(if bhckc502.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc502.value(row_i))
                    });
                    vals.push(if bhckc699.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc699.value(row_i))
                    });
                    vals.push(if bhckc779.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc779.value(row_i))
                    });
                    vals.push(if bhckc780.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc780.value(row_i))
                    });
                    vals.push(if bhckc866.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc866.value(row_i))
                    });
                    vals.push(if bhckc867.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc867.value(row_i))
                    });
                    vals.push(if bhckc868.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckc868.value(row_i))
                    });
                    vals.push(if bhckd957.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd957.value(row_i))
                    });
                    vals.push(if bhckd961.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd961.value(row_i))
                    });
                    vals.push(if bhckd966.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd966.value(row_i))
                    });
                    vals.push(if bhckd976.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd976.value(row_i))
                    });
                    vals.push(if bhckd977.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd977.value(row_i))
                    });
                    vals.push(if bhckd978.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd978.value(row_i))
                    });
                    vals.push(if bhckd979.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd979.value(row_i))
                    });
                    vals.push(if bhckd980.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd980.value(row_i))
                    });
                    vals.push(if bhckd981.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd981.value(row_i))
                    });
                    vals.push(if bhckd987.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd987.value(row_i))
                    });
                    vals.push(if bhckd988.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd988.value(row_i))
                    });
                    vals.push(if bhckd989.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd989.value(row_i))
                    });
                    vals.push(if bhckd990.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd990.value(row_i))
                    });
                    vals.push(if bhckd997.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd997.value(row_i))
                    });
                    vals.push(if bhckd998.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd998.value(row_i))
                    });
                    vals.push(if bhckd999.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckd999.value(row_i))
                    });
                    vals.push(if bhckf064.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf064.value(row_i))
                    });
                    vals.push(if bhckf065.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf065.value(row_i))
                    });
                    vals.push(if bhckf066.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf066.value(row_i))
                    });
                    vals.push(if bhckf067.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf067.value(row_i))
                    });
                    vals.push(if bhckf068.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf068.value(row_i))
                    });
                    vals.push(if bhckf069.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf069.value(row_i))
                    });
                    vals.push(if bhckf186.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf186.value(row_i))
                    });
                    vals.push(if bhckf187.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf187.value(row_i))
                    });
                    vals.push(if bhckf188.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf188.value(row_i))
                    });
                    vals.push(if bhckf230.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf230.value(row_i))
                    });
                    vals.push(if bhckf231.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf231.value(row_i))
                    });
                    vals.push(if bhckf232.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf232.value(row_i))
                    });
                    vals.push(if bhckf240.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf240.value(row_i))
                    });
                    vals.push(if bhckf243.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf243.value(row_i))
                    });
                    vals.push(if bhckf246.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf246.value(row_i))
                    });
                    vals.push(if bhckf249.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf249.value(row_i))
                    });
                    vals.push(if bhckf252.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf252.value(row_i))
                    });
                    vals.push(if bhckf255.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf255.value(row_i))
                    });
                    vals.push(if bhckf258.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf258.value(row_i))
                    });
                    vals.push(if bhckf261.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf261.value(row_i))
                    });
                    vals.push(if bhckf559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf559.value(row_i))
                    });
                    vals.push(if bhckf597.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf597.value(row_i))
                    });
                    vals.push(if bhckf598.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf598.value(row_i))
                    });
                    vals.push(if bhckf599.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf599.value(row_i))
                    });
                    vals.push(if bhckf600.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf600.value(row_i))
                    });
                    vals.push(if bhckf601.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf601.value(row_i))
                    });
                    vals.push(if bhckf609.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf609.value(row_i))
                    });
                    vals.push(if bhckf610.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf610.value(row_i))
                    });
                    vals.push(if bhckf614.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf614.value(row_i))
                    });
                    vals.push(if bhckf615.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf615.value(row_i))
                    });
                    vals.push(if bhckf616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf616.value(row_i))
                    });
                    vals.push(if bhckf617.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf617.value(row_i))
                    });
                    vals.push(if bhckf618.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf618.value(row_i))
                    });
                    vals.push(if bhckf624.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf624.value(row_i))
                    });
                    vals.push(if bhckf632.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf632.value(row_i))
                    });
                    vals.push(if bhckf633.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf633.value(row_i))
                    });
                    vals.push(if bhckf634.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf634.value(row_i))
                    });
                    vals.push(if bhckf635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf635.value(row_i))
                    });
                    vals.push(if bhckf636.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf636.value(row_i))
                    });
                    vals.push(if bhckf641.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf641.value(row_i))
                    });
                    vals.push(if bhckf642.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf642.value(row_i))
                    });
                    vals.push(if bhckf643.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf643.value(row_i))
                    });
                    vals.push(if bhckf644.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf644.value(row_i))
                    });
                    vals.push(if bhckf645.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf645.value(row_i))
                    });
                    vals.push(if bhckf646.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf646.value(row_i))
                    });
                    vals.push(if bhckf647.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf647.value(row_i))
                    });
                    vals.push(if bhckf648.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf648.value(row_i))
                    });
                    vals.push(if bhckf649.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf649.value(row_i))
                    });
                    vals.push(if bhckf650.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf650.value(row_i))
                    });
                    vals.push(if bhckf651.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf651.value(row_i))
                    });
                    vals.push(if bhckf652.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf652.value(row_i))
                    });
                    vals.push(if bhckf653.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf653.value(row_i))
                    });
                    vals.push(if bhckf654.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf654.value(row_i))
                    });
                    vals.push(if bhckf656.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf656.value(row_i))
                    });
                    vals.push(if bhckf657.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf657.value(row_i))
                    });
                    vals.push(if bhckf659.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf659.value(row_i))
                    });
                    vals.push(if bhckf660.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf660.value(row_i))
                    });
                    vals.push(if bhckf667.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf667.value(row_i))
                    });
                    vals.push(if bhckf668.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf668.value(row_i))
                    });
                    vals.push(if bhckf669.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf669.value(row_i))
                    });
                    vals.push(if bhckf699.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf699.value(row_i))
                    });
                    vals.push(if bhckf790.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf790.value(row_i))
                    });
                    vals.push(if bhckf837.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf837.value(row_i))
                    });
                    vals.push(if bhckf838.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckf838.value(row_i))
                    });
                    vals.push(if bhckf842.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckf842.value(row_i))
                    });
                    vals.push(if bhckft04.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft04.value(row_i))
                    });
                    vals.push(if bhckft05.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckft05.value(row_i))
                    });
                    vals.push(if bhckg105.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg105.value(row_i))
                    });
                    vals.push(if bhckg214.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg214.value(row_i))
                    });
                    vals.push(if bhckg215.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg215.value(row_i))
                    });
                    vals.push(if bhckg216.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg216.value(row_i))
                    });
                    vals.push(if bhckg217.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg217.value(row_i))
                    });
                    vals.push(if bhckg219.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg219.value(row_i))
                    });
                    vals.push(if bhckg220.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg220.value(row_i))
                    });
                    vals.push(if bhckg222.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg222.value(row_i))
                    });
                    vals.push(if bhckg299.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg299.value(row_i))
                    });
                    vals.push(if bhckg332.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg332.value(row_i))
                    });
                    vals.push(if bhckg333.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg333.value(row_i))
                    });
                    vals.push(if bhckg334.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg334.value(row_i))
                    });
                    vals.push(if bhckg335.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg335.value(row_i))
                    });
                    vals.push(if bhckg348.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg348.value(row_i))
                    });
                    vals.push(if bhckg349.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg349.value(row_i))
                    });
                    vals.push(if bhckg350.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg350.value(row_i))
                    });
                    vals.push(if bhckg351.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg351.value(row_i))
                    });
                    vals.push(if bhckg352.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg352.value(row_i))
                    });
                    vals.push(if bhckg353.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg353.value(row_i))
                    });
                    vals.push(if bhckg354.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg354.value(row_i))
                    });
                    vals.push(if bhckg355.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg355.value(row_i))
                    });
                    vals.push(if bhckg356.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg356.value(row_i))
                    });
                    vals.push(if bhckg357.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg357.value(row_i))
                    });
                    vals.push(if bhckg358.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg358.value(row_i))
                    });
                    vals.push(if bhckg359.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg359.value(row_i))
                    });
                    vals.push(if bhckg360.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg360.value(row_i))
                    });
                    vals.push(if bhckg361.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg361.value(row_i))
                    });
                    vals.push(if bhckg362.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg362.value(row_i))
                    });
                    vals.push(if bhckg363.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg363.value(row_i))
                    });
                    vals.push(if bhckg364.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg364.value(row_i))
                    });
                    vals.push(if bhckg365.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg365.value(row_i))
                    });
                    vals.push(if bhckg366.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg366.value(row_i))
                    });
                    vals.push(if bhckg367.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg367.value(row_i))
                    });
                    vals.push(if bhckg368.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg368.value(row_i))
                    });
                    vals.push(if bhckg369.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg369.value(row_i))
                    });
                    vals.push(if bhckg370.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg370.value(row_i))
                    });
                    vals.push(if bhckg371.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg371.value(row_i))
                    });
                    vals.push(if bhckg372.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg372.value(row_i))
                    });
                    vals.push(if bhckg373.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg373.value(row_i))
                    });
                    vals.push(if bhckg374.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg374.value(row_i))
                    });
                    vals.push(if bhckg375.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg375.value(row_i))
                    });
                    vals.push(if bhckg378.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg378.value(row_i))
                    });
                    vals.push(if bhckg379.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg379.value(row_i))
                    });
                    vals.push(if bhckg380.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg380.value(row_i))
                    });
                    vals.push(if bhckg381.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg381.value(row_i))
                    });
                    vals.push(if bhckg382.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg382.value(row_i))
                    });
                    vals.push(if bhckg383.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg383.value(row_i))
                    });
                    vals.push(if bhckg384.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg384.value(row_i))
                    });
                    vals.push(if bhckg385.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg385.value(row_i))
                    });
                    vals.push(if bhckg386.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg386.value(row_i))
                    });
                    vals.push(if bhckg387.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg387.value(row_i))
                    });
                    vals.push(if bhckg388.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg388.value(row_i))
                    });
                    vals.push(if bhckg418.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg418.value(row_i))
                    });
                    vals.push(if bhckg419.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg419.value(row_i))
                    });
                    vals.push(if bhckg420.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg420.value(row_i))
                    });
                    vals.push(if bhckg421.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg421.value(row_i))
                    });
                    vals.push(if bhckg422.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg422.value(row_i))
                    });
                    vals.push(if bhckg423.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg423.value(row_i))
                    });
                    vals.push(if bhckg424.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg424.value(row_i))
                    });
                    vals.push(if bhckg425.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg425.value(row_i))
                    });
                    vals.push(if bhckg426.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg426.value(row_i))
                    });
                    vals.push(if bhckg427.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg427.value(row_i))
                    });
                    vals.push(if bhckg428.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg428.value(row_i))
                    });
                    vals.push(if bhckg429.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg429.value(row_i))
                    });
                    vals.push(if bhckg430.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg430.value(row_i))
                    });
                    vals.push(if bhckg431.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg431.value(row_i))
                    });
                    vals.push(if bhckg432.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg432.value(row_i))
                    });
                    vals.push(if bhckg433.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg433.value(row_i))
                    });
                    vals.push(if bhckg434.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg434.value(row_i))
                    });
                    vals.push(if bhckg435.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg435.value(row_i))
                    });
                    vals.push(if bhckg436.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg436.value(row_i))
                    });
                    vals.push(if bhckg437.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg437.value(row_i))
                    });
                    vals.push(if bhckg438.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg438.value(row_i))
                    });
                    vals.push(if bhckg439.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg439.value(row_i))
                    });
                    vals.push(if bhckg440.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg440.value(row_i))
                    });
                    vals.push(if bhckg441.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg441.value(row_i))
                    });
                    vals.push(if bhckg442.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg442.value(row_i))
                    });
                    vals.push(if bhckg443.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg443.value(row_i))
                    });
                    vals.push(if bhckg444.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg444.value(row_i))
                    });
                    vals.push(if bhckg445.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg445.value(row_i))
                    });
                    vals.push(if bhckg446.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg446.value(row_i))
                    });
                    vals.push(if bhckg447.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg447.value(row_i))
                    });
                    vals.push(if bhckg448.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg448.value(row_i))
                    });
                    vals.push(if bhckg449.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg449.value(row_i))
                    });
                    vals.push(if bhckg450.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg450.value(row_i))
                    });
                    vals.push(if bhckg451.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg451.value(row_i))
                    });
                    vals.push(if bhckg452.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg452.value(row_i))
                    });
                    vals.push(if bhckg453.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg453.value(row_i))
                    });
                    vals.push(if bhckg454.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg454.value(row_i))
                    });
                    vals.push(if bhckg455.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg455.value(row_i))
                    });
                    vals.push(if bhckg456.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg456.value(row_i))
                    });
                    vals.push(if bhckg457.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg457.value(row_i))
                    });
                    vals.push(if bhckg458.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg458.value(row_i))
                    });
                    vals.push(if bhckg459.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg459.value(row_i))
                    });
                    vals.push(if bhckg460.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg460.value(row_i))
                    });
                    vals.push(if bhckg461.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg461.value(row_i))
                    });
                    vals.push(if bhckg462.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg462.value(row_i))
                    });
                    vals.push(if bhckg493.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg493.value(row_i))
                    });
                    vals.push(if bhckg494.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg494.value(row_i))
                    });
                    vals.push(if bhckg495.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg495.value(row_i))
                    });
                    vals.push(if bhckg496.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg496.value(row_i))
                    });
                    vals.push(if bhckg497.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg497.value(row_i))
                    });
                    vals.push(if bhckg498.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg498.value(row_i))
                    });
                    vals.push(if bhckg499.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg499.value(row_i))
                    });
                    vals.push(if bhckg500.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg500.value(row_i))
                    });
                    vals.push(if bhckg501.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg501.value(row_i))
                    });
                    vals.push(if bhckg502.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg502.value(row_i))
                    });
                    vals.push(if bhckg503.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg503.value(row_i))
                    });
                    vals.push(if bhckg504.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg504.value(row_i))
                    });
                    vals.push(if bhckg505.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg505.value(row_i))
                    });
                    vals.push(if bhckg506.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg506.value(row_i))
                    });
                    vals.push(if bhckg512.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg512.value(row_i))
                    });
                    vals.push(if bhckg513.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg513.value(row_i))
                    });
                    vals.push(if bhckg514.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg514.value(row_i))
                    });
                    vals.push(if bhckg515.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg515.value(row_i))
                    });
                    vals.push(if bhckg516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg516.value(row_i))
                    });
                    vals.push(if bhckg517.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg517.value(row_i))
                    });
                    vals.push(if bhckg518.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg518.value(row_i))
                    });
                    vals.push(if bhckg519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg519.value(row_i))
                    });
                    vals.push(if bhckg520.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg520.value(row_i))
                    });
                    vals.push(if bhckg526.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg526.value(row_i))
                    });
                    vals.push(if bhckg527.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg527.value(row_i))
                    });
                    vals.push(if bhckg528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg528.value(row_i))
                    });
                    vals.push(if bhckg529.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg529.value(row_i))
                    });
                    vals.push(if bhckg530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg530.value(row_i))
                    });
                    vals.push(if bhckg531.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg531.value(row_i))
                    });
                    vals.push(if bhckg532.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg532.value(row_i))
                    });
                    vals.push(if bhckg533.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg533.value(row_i))
                    });
                    vals.push(if bhckg534.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg534.value(row_i))
                    });
                    vals.push(if bhckg535.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg535.value(row_i))
                    });
                    vals.push(if bhckg551.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg551.value(row_i))
                    });
                    vals.push(if bhckg552.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg552.value(row_i))
                    });
                    vals.push(if bhckg553.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg553.value(row_i))
                    });
                    vals.push(if bhckg554.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg554.value(row_i))
                    });
                    vals.push(if bhckg555.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg555.value(row_i))
                    });
                    vals.push(if bhckg556.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg556.value(row_i))
                    });
                    vals.push(if bhckg557.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg557.value(row_i))
                    });
                    vals.push(if bhckg558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg558.value(row_i))
                    });
                    vals.push(if bhckg559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg559.value(row_i))
                    });
                    vals.push(if bhckg560.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg560.value(row_i))
                    });
                    vals.push(if bhckg576.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg576.value(row_i))
                    });
                    vals.push(if bhckg577.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg577.value(row_i))
                    });
                    vals.push(if bhckg578.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg578.value(row_i))
                    });
                    vals.push(if bhckg579.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg579.value(row_i))
                    });
                    vals.push(if bhckg580.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg580.value(row_i))
                    });
                    vals.push(if bhckg581.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg581.value(row_i))
                    });
                    vals.push(if bhckg582.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg582.value(row_i))
                    });
                    vals.push(if bhckg583.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg583.value(row_i))
                    });
                    vals.push(if bhckg584.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg584.value(row_i))
                    });
                    vals.push(if bhckg585.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg585.value(row_i))
                    });
                    vals.push(if bhckg591.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg591.value(row_i))
                    });
                    vals.push(if bhckg603.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg603.value(row_i))
                    });
                    vals.push(if bhckg604.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg604.value(row_i))
                    });
                    vals.push(if bhckg605.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg605.value(row_i))
                    });
                    vals.push(if bhckg612.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg612.value(row_i))
                    });
                    vals.push(if bhckg613.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg613.value(row_i))
                    });
                    vals.push(if bhckg614.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg614.value(row_i))
                    });
                    vals.push(if bhckg615.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg615.value(row_i))
                    });
                    vals.push(if bhckg616.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg616.value(row_i))
                    });
                    vals.push(if bhckg617.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg617.value(row_i))
                    });
                    vals.push(if bhckg624.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg624.value(row_i))
                    });
                    vals.push(if bhckg625.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg625.value(row_i))
                    });
                    vals.push(if bhckg626.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg626.value(row_i))
                    });
                    vals.push(if bhckg627.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg627.value(row_i))
                    });
                    vals.push(if bhckg628.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg628.value(row_i))
                    });
                    vals.push(if bhckg629.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg629.value(row_i))
                    });
                    vals.push(if bhckg630.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg630.value(row_i))
                    });
                    vals.push(if bhckg631.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg631.value(row_i))
                    });
                    vals.push(if bhckg632.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg632.value(row_i))
                    });
                    vals.push(if bhckg633.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg633.value(row_i))
                    });
                    vals.push(if bhckg634.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg634.value(row_i))
                    });
                    vals.push(if bhckg635.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg635.value(row_i))
                    });
                    vals.push(if bhckg636.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg636.value(row_i))
                    });
                    vals.push(if bhckg637.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg637.value(row_i))
                    });
                    vals.push(if bhckg641.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg641.value(row_i))
                    });
                    vals.push(if bhckg651.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg651.value(row_i))
                    });
                    vals.push(if bhckg652.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckg652.value(row_i))
                    });
                    vals.push(if bhckh171.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh171.value(row_i))
                    });
                    vals.push(if bhckh191.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh191.value(row_i))
                    });
                    vals.push(if bhckh289.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh289.value(row_i))
                    });
                    vals.push(if bhckh290.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh290.value(row_i))
                    });
                    vals.push(if bhckh291.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh291.value(row_i))
                    });
                    vals.push(if bhckh292.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh292.value(row_i))
                    });
                    vals.push(if bhckh300.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh300.value(row_i))
                    });
                    vals.push(if bhckh301.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh301.value(row_i))
                    });
                    vals.push(if bhckh302.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh302.value(row_i))
                    });
                    vals.push(if bhckh303.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh303.value(row_i))
                    });
                    vals.push(if bhckh304.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh304.value(row_i))
                    });
                    vals.push(if bhckh307.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh307.value(row_i))
                    });
                    vals.push(if bhckh308.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh308.value(row_i))
                    });
                    vals.push(if bhckh309.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh309.value(row_i))
                    });
                    vals.push(if bhckh310.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckh310.value(row_i))
                    });
                    vals.push(if bhckhj74.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj74.value(row_i))
                    });
                    vals.push(if bhckhj75.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj75.value(row_i))
                    });
                    vals.push(if bhckhj76.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj76.value(row_i))
                    });
                    vals.push(if bhckhj77.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj77.value(row_i))
                    });
                    vals.push(if bhckhj86.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj86.value(row_i))
                    });
                    vals.push(if bhckhj87.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj87.value(row_i))
                    });
                    vals.push(if bhckhj90.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj90.value(row_i))
                    });
                    vals.push(if bhckhj91.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj91.value(row_i))
                    });
                    vals.push(if bhckhj96.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj96.value(row_i))
                    });
                    vals.push(if bhckhj97.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj97.value(row_i))
                    });
                    vals.push(if bhckhj98.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj98.value(row_i))
                    });
                    vals.push(if bhckhj99.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhj99.value(row_i))
                    });
                    vals.push(if bhckhk00.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk00.value(row_i))
                    });
                    vals.push(if bhckhk01.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk01.value(row_i))
                    });
                    vals.push(if bhckhk25.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk25.value(row_i))
                    });
                    vals.push(if bhckhk26.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk26.value(row_i))
                    });
                    vals.push(if bhckhk27.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk27.value(row_i))
                    });
                    vals.push(if bhckhk28.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhk28.value(row_i))
                    });
                    vals.push(if bhckht50.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht50.value(row_i))
                    });
                    vals.push(if bhckht51.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht51.value(row_i))
                    });
                    vals.push(if bhckht52.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht52.value(row_i))
                    });
                    vals.push(if bhckht53.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht53.value(row_i))
                    });
                    vals.push(if bhckht66.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht66.value(row_i))
                    });
                    vals.push(if bhckht67.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht67.value(row_i))
                    });
                    vals.push(if bhckht68.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht68.value(row_i))
                    });
                    vals.push(if bhckht70.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht70.value(row_i))
                    });
                    vals.push(if bhckht81.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht81.value(row_i))
                    });
                    vals.push(if bhckht82.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht82.value(row_i))
                    });
                    vals.push(if bhckht86.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckht86.value(row_i))
                    });
                    vals.push(if bhckhu16.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu16.value(row_i))
                    });
                    vals.push(if bhckhu17.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu17.value(row_i))
                    });
                    vals.push(if bhckhu18.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckhu18.value(row_i))
                    });
                    vals.push(if bhckj319.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj319.value(row_i))
                    });
                    vals.push(if bhckj321.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj321.value(row_i))
                    });
                    vals.push(if bhckj457.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj457.value(row_i))
                    });
                    vals.push(if bhckj458.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj458.value(row_i))
                    });
                    vals.push(if bhckj459.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckj459.value(row_i))
                    });
                    vals.push(if bhckjf77.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf77.value(row_i))
                    });
                    vals.push(if bhckjf78.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjf78.value(row_i))
                    });
                    vals.push(if bhckjh89.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh89.value(row_i))
                    });
                    vals.push(if bhckjh90.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh90.value(row_i))
                    });
                    vals.push(if bhckjh95.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh95.value(row_i))
                    });
                    vals.push(if bhckjh96.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjh96.value(row_i))
                    });
                    vals.push(if bhckjj02.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj02.value(row_i))
                    });
                    vals.push(if bhckjj33.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckjj33.value(row_i))
                    });
                    vals.push(if bhckk042.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk042.value(row_i))
                    });
                    vals.push(if bhckk043.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk043.value(row_i))
                    });
                    vals.push(if bhckk044.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk044.value(row_i))
                    });
                    vals.push(if bhckk102.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk102.value(row_i))
                    });
                    vals.push(if bhckk103.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk103.value(row_i))
                    });
                    vals.push(if bhckk104.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk104.value(row_i))
                    });
                    vals.push(if bhckk133.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk133.value(row_i))
                    });
                    vals.push(if bhckk141.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk141.value(row_i))
                    });
                    vals.push(if bhckk195.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk195.value(row_i))
                    });
                    vals.push(if bhckk197.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk197.value(row_i))
                    });
                    vals.push(if bhckk198.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk198.value(row_i))
                    });
                    vals.push(if bhckk199.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk199.value(row_i))
                    });
                    vals.push(if bhckk200.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk200.value(row_i))
                    });
                    vals.push(if bhckk206.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk206.value(row_i))
                    });
                    vals.push(if bhckk209.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk209.value(row_i))
                    });
                    vals.push(if bhckk210.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk210.value(row_i))
                    });
                    vals.push(if bhckk211.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckk211.value(row_i))
                    });
                    vals.push(if bhckkx48.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx48.value(row_i))
                    });
                    vals.push(if bhckkx49.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx49.value(row_i))
                    });
                    vals.push(if bhckkx56.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx56.value(row_i))
                    });
                    vals.push(if bhckkx59.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx59.value(row_i))
                    });
                    vals.push(if bhckkx66.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx66.value(row_i))
                    });
                    vals.push(if bhckkx67.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx67.value(row_i))
                    });
                    vals.push(if bhckkx68.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckkx68.value(row_i))
                    });
                    vals.push(if bhckl183.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckl183.value(row_i))
                    });
                    vals.push(if bhckl184.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckl184.value(row_i))
                    });
                    vals.push(if bhckl185.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckl185.value(row_i))
                    });
                    vals.push(if bhckl186.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckl186.value(row_i))
                    });
                    vals.push(if bhckl187.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckl187.value(row_i))
                    });
                    vals.push(if bhckl188.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckl188.value(row_i))
                    });
                    vals.push(if bhckl191.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckl191.value(row_i))
                    });
                    vals.push(if bhckl192.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhckl192.value(row_i))
                    });
                    vals.push(if bhckle75.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckle75.value(row_i))
                    });
                    vals.push(if bhcklg25.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Boolean(bhcklg25.value(row_i))
                    });
                    vals.push(if bhcklg27.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcklg27.value(row_i))
                    });
                    vals.push(if bhcklg28.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcklg28.value(row_i))
                    });
                    vals.push(if bhckll57.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckll57.value(row_i))
                    });
                    vals.push(if bhckm288.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm288.value(row_i))
                    });
                    vals.push(if bhckm708.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm708.value(row_i))
                    });
                    vals.push(if bhckm709.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm709.value(row_i))
                    });
                    vals.push(if bhckm710.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm710.value(row_i))
                    });
                    vals.push(if bhckm711.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm711.value(row_i))
                    });
                    vals.push(if bhckm712.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm712.value(row_i))
                    });
                    vals.push(if bhckm713.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm713.value(row_i))
                    });
                    vals.push(if bhckm714.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm714.value(row_i))
                    });
                    vals.push(if bhckm715.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm715.value(row_i))
                    });
                    vals.push(if bhckm716.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm716.value(row_i))
                    });
                    vals.push(if bhckm717.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm717.value(row_i))
                    });
                    vals.push(if bhckm719.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm719.value(row_i))
                    });
                    vals.push(if bhckm720.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm720.value(row_i))
                    });
                    vals.push(if bhckm721.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm721.value(row_i))
                    });
                    vals.push(if bhckm722.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm722.value(row_i))
                    });
                    vals.push(if bhckm723.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm723.value(row_i))
                    });
                    vals.push(if bhckm724.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm724.value(row_i))
                    });
                    vals.push(if bhckm725.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm725.value(row_i))
                    });
                    vals.push(if bhckm726.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm726.value(row_i))
                    });
                    vals.push(if bhckm745.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm745.value(row_i))
                    });
                    vals.push(if bhckm746.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm746.value(row_i))
                    });
                    vals.push(if bhckm747.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm747.value(row_i))
                    });
                    vals.push(if bhckm748.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm748.value(row_i))
                    });
                    vals.push(if bhckm749.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm749.value(row_i))
                    });
                    vals.push(if bhckm750.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm750.value(row_i))
                    });
                    vals.push(if bhckm751.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckm751.value(row_i))
                    });
                    vals.push(if bhckmg93.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckmg93.value(row_i))
                    });
                    vals.push(if bhckmg95.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhckmg95.value(row_i))
                    });
                    vals.push(if bhcks413.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks413.value(row_i))
                    });
                    vals.push(if bhcks419.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks419.value(row_i))
                    });
                    vals.push(if bhcks423.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks423.value(row_i))
                    });
                    vals.push(if bhcks431.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks431.value(row_i))
                    });
                    vals.push(if bhcks439.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks439.value(row_i))
                    });
                    vals.push(if bhcks445.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks445.value(row_i))
                    });
                    vals.push(if bhcks449.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks449.value(row_i))
                    });
                    vals.push(if bhcks457.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks457.value(row_i))
                    });
                    vals.push(if bhcks466.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks466.value(row_i))
                    });
                    vals.push(if bhcks467.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks467.value(row_i))
                    });
                    vals.push(if bhcks475.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks475.value(row_i))
                    });
                    vals.push(if bhcks480.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks480.value(row_i))
                    });
                    vals.push(if bhcks485.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks485.value(row_i))
                    });
                    vals.push(if bhcks490.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks490.value(row_i))
                    });
                    vals.push(if bhcks495.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks495.value(row_i))
                    });
                    vals.push(if bhcks500.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks500.value(row_i))
                    });
                    vals.push(if bhcks503.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks503.value(row_i))
                    });
                    vals.push(if bhcks504.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks504.value(row_i))
                    });
                    vals.push(if bhcks505.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks505.value(row_i))
                    });
                    vals.push(if bhcks506.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks506.value(row_i))
                    });
                    vals.push(if bhcks507.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks507.value(row_i))
                    });
                    vals.push(if bhcks510.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks510.value(row_i))
                    });
                    vals.push(if bhcks512.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks512.value(row_i))
                    });
                    vals.push(if bhcks514.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks514.value(row_i))
                    });
                    vals.push(if bhcks515.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks515.value(row_i))
                    });
                    vals.push(if bhcks516.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks516.value(row_i))
                    });
                    vals.push(if bhcks517.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks517.value(row_i))
                    });
                    vals.push(if bhcks518.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks518.value(row_i))
                    });
                    vals.push(if bhcks519.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks519.value(row_i))
                    });
                    vals.push(if bhcks520.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks520.value(row_i))
                    });
                    vals.push(if bhcks521.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks521.value(row_i))
                    });
                    vals.push(if bhcks522.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks522.value(row_i))
                    });
                    vals.push(if bhcks523.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks523.value(row_i))
                    });
                    vals.push(if bhcks525.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks525.value(row_i))
                    });
                    vals.push(if bhcks526.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks526.value(row_i))
                    });
                    vals.push(if bhcks527.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks527.value(row_i))
                    });
                    vals.push(if bhcks528.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks528.value(row_i))
                    });
                    vals.push(if bhcks529.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks529.value(row_i))
                    });
                    vals.push(if bhcks530.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks530.value(row_i))
                    });
                    vals.push(if bhcks531.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks531.value(row_i))
                    });
                    vals.push(if bhcks539.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks539.value(row_i))
                    });
                    vals.push(if bhcks540.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks540.value(row_i))
                    });
                    vals.push(if bhcks541.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks541.value(row_i))
                    });
                    vals.push(if bhcks542.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks542.value(row_i))
                    });
                    vals.push(if bhcks543.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks543.value(row_i))
                    });
                    vals.push(if bhcks544.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks544.value(row_i))
                    });
                    vals.push(if bhcks545.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks545.value(row_i))
                    });
                    vals.push(if bhcks546.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks546.value(row_i))
                    });
                    vals.push(if bhcks547.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks547.value(row_i))
                    });
                    vals.push(if bhcks548.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks548.value(row_i))
                    });
                    vals.push(if bhcks558.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks558.value(row_i))
                    });
                    vals.push(if bhcks559.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks559.value(row_i))
                    });
                    vals.push(if bhcks560.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks560.value(row_i))
                    });
                    vals.push(if bhcks561.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks561.value(row_i))
                    });
                    vals.push(if bhcks562.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks562.value(row_i))
                    });
                    vals.push(if bhcks563.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks563.value(row_i))
                    });
                    vals.push(if bhcks564.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks564.value(row_i))
                    });
                    vals.push(if bhcks565.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks565.value(row_i))
                    });
                    vals.push(if bhcks566.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks566.value(row_i))
                    });
                    vals.push(if bhcks567.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks567.value(row_i))
                    });
                    vals.push(if bhcks568.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks568.value(row_i))
                    });
                    vals.push(if bhcks569.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks569.value(row_i))
                    });
                    vals.push(if bhcks570.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks570.value(row_i))
                    });
                    vals.push(if bhcks571.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks571.value(row_i))
                    });
                    vals.push(if bhcks572.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks572.value(row_i))
                    });
                    vals.push(if bhcks573.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks573.value(row_i))
                    });
                    vals.push(if bhcks574.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks574.value(row_i))
                    });
                    vals.push(if bhcks575.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks575.value(row_i))
                    });
                    vals.push(if bhcks576.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks576.value(row_i))
                    });
                    vals.push(if bhcks577.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks577.value(row_i))
                    });
                    vals.push(if bhcks578.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks578.value(row_i))
                    });
                    vals.push(if bhcks579.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks579.value(row_i))
                    });
                    vals.push(if bhcks580.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks580.value(row_i))
                    });
                    vals.push(if bhcks581.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks581.value(row_i))
                    });
                    vals.push(if bhcks624.is_null(row_i) {
                        AnyValue::Null
                    } else {
                        AnyValue::Float64(bhcks624.value(row_i))
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
