use super::{AppError, DuckCrudModel};
use arrow_array::{Array, Float64Array, Int64Array, StringArray};
use duckdb::Connection;
use polars::frame::row::Row;
use polars::prelude::*;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Wide World Development Indicators row: one indicator-year with country/region values.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct WdiWide {
    pub indicator_name: String,
    pub indicator_code: String,
    pub year: i32,
    // Regions and aggregates
    pub afe: Option<f64>, // Africa Eastern and Southern
    pub afw: Option<f64>, // Africa Western and Central
    pub arb: Option<f64>, // Arab World
    pub css: Option<f64>, // Caribbean small states
    pub ceb: Option<f64>, // Central Europe and the Baltics
    pub ear: Option<f64>, // Region aggregate (EAR)
    pub eas: Option<f64>, // East Asia & Pacific
    pub eap: Option<f64>, // East Asia & Pacific (IDA & IBRD)
    pub tea: Option<f64>, // East Asia & Pacific (IDA & IBRD) aggregate
    pub emu: Option<f64>, // Euro area
    pub ecs: Option<f64>, // Europe & Central Asia (IDA & IBRD)
    pub eca: Option<f64>, // Europe & Central Asia
    pub tec: Option<f64>, // Europe & Central Asia (IDA & IBRD) aggregate
    pub euu: Option<f64>, // European Union
    pub fcs: Option<f64>, // Fragile and conflict-affected situations
    pub hpc: Option<f64>, // Heavily indebted poor countries (HIPC)
    pub hic: Option<f64>, // High income
    pub ibd: Option<f64>, // IBRD only
    pub ibt: Option<f64>, // IDA & IBRD total
    pub idb: Option<f64>, // IDA blend
    pub idx: Option<f64>, // IDA total (verify)
    pub ida: Option<f64>, // IDA only
    pub lte: Option<f64>, // Region aggregate (LTE)
    pub lcn: Option<f64>, // Latin America & Caribbean (IDA & IBRD)
    pub lac: Option<f64>, // Latin America & Caribbean
    pub tla: Option<f64>, // Latin America & Caribbean aggregate
    pub ldc: Option<f64>, // Least developed countries: UN classification
    pub lmy: Option<f64>, // Low & middle income
    pub lic: Option<f64>, // Low income
    pub lmc: Option<f64>, // Lower middle income
    pub mea: Option<f64>, // Middle East & North Africa (IDA & IBRD)
    pub mna: Option<f64>, // Middle East & North Africa
    pub tmn: Option<f64>, // Middle East & North Africa aggregate
    pub mic: Option<f64>, // Middle income
    pub nac: Option<f64>, // North America
    pub inx: Option<f64>, // Not classified
    pub oed: Option<f64>, // OECD members
    pub oss: Option<f64>, // Other small states
    pub pss: Option<f64>, // Pacific island small states
    pub pst: Option<f64>, // Post-demographic dividend
    pub pre: Option<f64>, // Pre-demographic dividend
    pub sst: Option<f64>, // Sub-Saharan Africa (aggregate)
    pub sas: Option<f64>, // South Asia
    pub tsa: Option<f64>, // South Asia (IDA & IBRD)
    pub ssf: Option<f64>, // Sub-Saharan Africa (IDA & IBRD)
    pub ssa: Option<f64>, // Sub-Saharan Africa
    pub tss: Option<f64>, // Sub-Saharan Africa aggregate
    pub umc: Option<f64>, // Upper middle income
    pub wld: Option<f64>, // World

    // Countries and territories
    pub afg: Option<f64>, // Afghanistan
    pub alb: Option<f64>, // Albania
    pub dza: Option<f64>, // Algeria
    pub asm: Option<f64>, // American Samoa
    pub and: Option<f64>, // Andorra
    pub ago: Option<f64>, // Angola
    pub atg: Option<f64>, // Antigua and Barbuda
    pub arg: Option<f64>, // Argentina
    pub arm: Option<f64>, // Armenia
    pub abw: Option<f64>, // Aruba
    pub aus: Option<f64>, // Australia
    pub aut: Option<f64>, // Austria
    pub aze: Option<f64>, // Azerbaijan
    pub bhs: Option<f64>, // Bahamas, The
    pub bhr: Option<f64>, // Bahrain
    pub bgd: Option<f64>, // Bangladesh
    pub brb: Option<f64>, // Barbados
    pub blr: Option<f64>, // Belarus
    pub bel: Option<f64>, // Belgium
    pub blz: Option<f64>, // Belize
    pub ben: Option<f64>, // Benin
    pub bmu: Option<f64>, // Bermuda
    pub btn: Option<f64>, // Bhutan
    pub bol: Option<f64>, // Bolivia
    pub bih: Option<f64>, // Bosnia and Herzegovina
    pub bwa: Option<f64>, // Botswana
    pub bra: Option<f64>, // Brazil
    pub vgb: Option<f64>, // British Virgin Islands
    pub brn: Option<f64>, // Brunei Darussalam
    pub bgr: Option<f64>, // Bulgaria
    pub bfa: Option<f64>, // Burkina Faso
    pub bdi: Option<f64>, // Burundi
    pub cpv: Option<f64>, // Cabo Verde
    pub khm: Option<f64>, // Cambodia
    pub cmr: Option<f64>, // Cameroon
    pub can: Option<f64>, // Canada
    pub cym: Option<f64>, // Cayman Islands
    pub caf: Option<f64>, // Central African Republic
    pub tcd: Option<f64>, // Chad
    pub chi: Option<f64>, // Channel Islands
    pub chl: Option<f64>, // Chile
    pub chn: Option<f64>, // China
    pub col: Option<f64>, // Colombia
    pub com: Option<f64>, // Comoros
    pub cod: Option<f64>, // Congo, Dem. Rep.
    pub cog: Option<f64>, // Congo, Rep.
    pub cri: Option<f64>, // Costa Rica
    pub civ: Option<f64>, // Cote d'Ivoire
    pub hrv: Option<f64>, // Croatia
    pub cub: Option<f64>, // Cuba
    pub cuw: Option<f64>, // Curacao
    pub cyp: Option<f64>, // Cyprus
    pub cze: Option<f64>, // Czechia
    pub dnk: Option<f64>, // Denmark
    pub dji: Option<f64>, // Djibouti
    pub dma: Option<f64>, // Dominica
    pub dom: Option<f64>, // Dominican Republic
    pub ecu: Option<f64>, // Ecuador
    pub egy: Option<f64>, // Egypt, Arab Rep.
    pub slv: Option<f64>, // El Salvador
    pub gnq: Option<f64>, // Equatorial Guinea
    pub eri: Option<f64>, // Eritrea
    pub est: Option<f64>, // Estonia
    pub swz: Option<f64>, // Eswatini
    pub eth: Option<f64>, // Ethiopia
    pub fro: Option<f64>, // Faroe Islands
    pub fji: Option<f64>, // Fiji
    pub fin: Option<f64>, // Finland
    pub fra: Option<f64>, // France
    pub pyf: Option<f64>, // French Polynesia
    pub gab: Option<f64>, // Gabon
    pub gmb: Option<f64>, // Gambia, The
    pub geo: Option<f64>, // Georgia
    pub deu: Option<f64>, // Germany
    pub gha: Option<f64>, // Ghana
    pub gib: Option<f64>, // Gibraltar
    pub grc: Option<f64>, // Greece
    pub grl: Option<f64>, // Greenland
    pub grd: Option<f64>, // Grenada
    pub gum: Option<f64>, // Guam
    pub gtm: Option<f64>, // Guatemala
    pub gin: Option<f64>, // Guinea
    pub gnb: Option<f64>, // Guinea-Bissau
    pub guy: Option<f64>, // Guyana
    pub hti: Option<f64>, // Haiti
    pub hnd: Option<f64>, // Honduras
    pub hkg: Option<f64>, // Hong Kong SAR, China
    pub hun: Option<f64>, // Hungary
    pub isl: Option<f64>, // Iceland
    pub ind: Option<f64>, // India
    pub idn: Option<f64>, // Indonesia
    pub irn: Option<f64>, // Iran, Islamic Rep.
    pub irq: Option<f64>, // Iraq
    pub irl: Option<f64>, // Ireland
    pub imn: Option<f64>, // Isle of Man
    pub isr: Option<f64>, // Israel
    pub ita: Option<f64>, // Italy
    pub jam: Option<f64>, // Jamaica
    pub jpn: Option<f64>, // Japan
    pub jor: Option<f64>, // Jordan
    pub kaz: Option<f64>, // Kazakhstan
    pub ken: Option<f64>, // Kenya
    pub kir: Option<f64>, // Kiribati
    pub prk: Option<f64>, // Korea, Dem. People's Rep.
    pub kor: Option<f64>, // Korea, Rep.
    pub xkx: Option<f64>, // Kosovo
    pub kwt: Option<f64>, // Kuwait
    pub kgz: Option<f64>, // Kyrgyz Republic
    pub lao: Option<f64>, // Lao PDR
    pub lva: Option<f64>, // Latvia
    pub lbn: Option<f64>, // Lebanon
    pub lso: Option<f64>, // Lesotho
    pub lbr: Option<f64>, // Liberia
    pub lby: Option<f64>, // Libya
    pub lie: Option<f64>, // Liechtenstein
    pub ltu: Option<f64>, // Lithuania
    pub lux: Option<f64>, // Luxembourg
    pub mac: Option<f64>, // Macao SAR, China
    pub mdg: Option<f64>, // Madagascar
    pub mwi: Option<f64>, // Malawi
    pub mys: Option<f64>, // Malaysia
    pub mdv: Option<f64>, // Maldives
    pub mli: Option<f64>, // Mali
    pub mlt: Option<f64>, // Malta
    pub mhl: Option<f64>, // Marshall Islands
    pub mrt: Option<f64>, // Mauritania
    pub mus: Option<f64>, // Mauritius
    pub mex: Option<f64>, // Mexico
    pub fsm: Option<f64>, // Micronesia, Fed. Sts.
    pub mda: Option<f64>, // Moldova
    pub mco: Option<f64>, // Monaco
    pub mng: Option<f64>, // Mongolia
    pub mne: Option<f64>, // Montenegro
    pub mar: Option<f64>, // Morocco
    pub moz: Option<f64>, // Mozambique
    pub mmr: Option<f64>, // Myanmar
    pub nam: Option<f64>, // Namibia
    pub nru: Option<f64>, // Nauru
    pub npl: Option<f64>, // Nepal
    pub nld: Option<f64>, // Netherlands
    pub ncl: Option<f64>, // New Caledonia
    pub nzl: Option<f64>, // New Zealand
    pub nic: Option<f64>, // Nicaragua
    pub ner: Option<f64>, // Niger
    pub nga: Option<f64>, // Nigeria
    pub mkd: Option<f64>, // North Macedonia
    pub mnp: Option<f64>, // Northern Mariana Islands
    pub nor: Option<f64>, // Norway
    pub omn: Option<f64>, // Oman
    pub pak: Option<f64>, // Pakistan
    pub plw: Option<f64>, // Palau
    pub pan: Option<f64>, // Panama
    pub png: Option<f64>, // Papua New Guinea
    pub pry: Option<f64>, // Paraguay
    pub per: Option<f64>, // Peru
    pub phl: Option<f64>, // Philippines
    pub pol: Option<f64>, // Poland
    pub prt: Option<f64>, // Portugal
    pub pri: Option<f64>, // Puerto Rico
    pub qat: Option<f64>, // Qatar
    pub rou: Option<f64>, // Romania
    pub rus: Option<f64>, // Russian Federation
    pub rwa: Option<f64>, // Rwanda
    pub wsm: Option<f64>, // Samoa
    pub smr: Option<f64>, // San Marino
    pub stp: Option<f64>, // Sao Tome and Principe
    pub sau: Option<f64>, // Saudi Arabia
    pub sen: Option<f64>, // Senegal
    pub srb: Option<f64>, // Serbia
    pub syc: Option<f64>, // Seychelles
    pub sle: Option<f64>, // Sierra Leone
    pub sgp: Option<f64>, // Singapore
    pub sxm: Option<f64>, // Sint Maarten (Dutch part)
    pub svk: Option<f64>, // Slovak Republic
    pub svn: Option<f64>, // Slovenia
    pub slb: Option<f64>, // Solomon Islands
    pub som: Option<f64>, // Somalia
    pub zaf: Option<f64>, // South Africa
    pub ssd: Option<f64>, // South Sudan
    pub esp: Option<f64>, // Spain
    pub lka: Option<f64>, // Sri Lanka
    pub kna: Option<f64>, // St. Kitts and Nevis
    pub lca: Option<f64>, // St. Lucia
    pub maf: Option<f64>, // St. Martin (French part)
    pub vct: Option<f64>, // St. Vincent and the Grenadines
    pub sdn: Option<f64>, // Sudan
    pub sur: Option<f64>, // Suriname
    pub swe: Option<f64>, // Sweden
    pub che: Option<f64>, // Switzerland
    pub syr: Option<f64>, // Syrian Arab Republic
    pub tjk: Option<f64>, // Tajikistan
    pub tza: Option<f64>, // Tanzania
    pub tha: Option<f64>, // Thailand
    pub tls: Option<f64>, // Timor-Leste
    pub tgo: Option<f64>, // Togo
    pub ton: Option<f64>, // Tonga
    pub tto: Option<f64>, // Trinidad and Tobago
    pub tun: Option<f64>, // Tunisia
    pub tur: Option<f64>, // Turkiye
    pub tkm: Option<f64>, // Turkmenistan
    pub tca: Option<f64>, // Turks and Caicos Islands
    pub tuv: Option<f64>, // Tuvalu
    pub uga: Option<f64>, // Uganda
    pub ukr: Option<f64>, // Ukraine
    pub are: Option<f64>, // United Arab Emirates
    pub gbr: Option<f64>, // United Kingdom
    pub usa: Option<f64>, // United States
    pub ury: Option<f64>, // Uruguay
    pub uzb: Option<f64>, // Uzbekistan
    pub vut: Option<f64>, // Vanuatu
    pub ven: Option<f64>, // Venezuela, RB
    pub vnm: Option<f64>, // Vietnam
    pub vir: Option<f64>, // Virgin Islands (U.S.)
    pub pse: Option<f64>, // West Bank and Gaza
    pub yem: Option<f64>, // Yemen, Rep.
    pub zmb: Option<f64>, // Zambia
    pub zwe: Option<f64>, // Zimbabwe
}

impl DuckCrudModel for WdiWide {
    fn table() -> &'static str {
        // Default table name when not specified explicitly.
        "wdi_wide"
    }
}

impl WdiWide {
    /// Ingest a WDI wide parquet into DuckDB (create/replace table).
    ///
    /// - `parquet_path`: path to a parquet with columns like
    ///   "Indicator Name", "Indicator Code", "Year", plus country/region codes (e.g., AUS, USA, WLD...).
    /// - `table_name`: optional override for the target table name. If `None`, uses `wdi_wide`.
    pub async fn duck_from_parquet(
        conn: Arc<Mutex<Connection>>,
        parquet_path: impl AsRef<Path>,
        table_name: Option<&str>,
    ) -> Result<usize, AppError> {
        let table_str = table_name
            .map(|s| s.to_string())
            .unwrap_or_else(|| <Self as DuckCrudModel>::table().into());
        <Self as DuckCrudModel>::upsert_from_parquet_one_file(
            conn,
            parquet_path,
            None,
            Some(table_str),
        )
        .await
    }

    /// Convert the full wide struct into a Polars Row following the struct field order.
    pub fn to_row<'a>(self) -> Row<'a> {
        let mut vals: Vec<AnyValue<'a>> = Vec::with_capacity(3 + 269); // rough capacity
        vals.push(AnyValue::StringOwned(self.indicator_name.into()));
        vals.push(AnyValue::StringOwned(self.indicator_code.into()));
        vals.push(AnyValue::Int32(self.year));

        // Regions/aggregates
        vals.push(self.afe.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.afw.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.arb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.css.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ceb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ear.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.eas.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.eap.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tea.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.emu.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ecs.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.eca.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tec.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.euu.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fcs.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.hpc.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.hic.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ibt.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.idb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.idx.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ida.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lte.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lcn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lac.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tla.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ldc.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lmy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lic.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lmc.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mea.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mna.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tmn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mic.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nac.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.inx.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.oed.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.oss.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pss.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pst.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pre.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sst.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sas.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tsa.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ssf.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ssa.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tss.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.umc.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.wld.map_or(AnyValue::Null, AnyValue::Float64));

        // Countries/territories
        vals.push(self.afg.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.alb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dza.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.asm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.and.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ago.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.atg.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.arg.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.arm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.abw.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.aus.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.aut.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.aze.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bhs.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bhr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bgd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.brb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.blr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bel.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.blz.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ben.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bmu.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.btn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bol.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bih.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bwa.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bra.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.vgb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.brn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bgr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bfa.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.bdi.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cpv.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.khm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cmr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.can.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cym.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.caf.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tcd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.chi.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.chl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.chn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.col.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.com.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cod.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cog.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cri.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.civ.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.hrv.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cub.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cuw.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cyp.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.cze.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dnk.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dji.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dma.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.dom.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ecu.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.egy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.slv.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gnq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.eri.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.est.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.swz.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.eth.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fro.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fji.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fin.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fra.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pyf.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gab.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gmb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.geo.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.deu.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gha.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gib.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.grc.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.grl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.grd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gum.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gtm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gin.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gnb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.guy.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.hti.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.hnd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.hkg.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.hun.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.isl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ind.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.idn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.irn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.irq.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.irl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.imn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.isr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ita.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.jam.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.jpn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.jor.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.kaz.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ken.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.kir.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.prk.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.kor.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.xkx.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.kwt.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.kgz.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lao.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lva.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lbn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lso.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lbr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lby.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lie.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ltu.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lux.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mac.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mdg.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mwi.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mys.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mdv.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mli.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mlt.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mhl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mrt.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mus.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mex.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.fsm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mda.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mco.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mng.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mne.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mar.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.moz.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mmr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nam.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nru.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.npl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nld.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ncl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nzl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nic.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ner.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nga.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mkd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.mnp.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.nor.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.omn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pak.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.plw.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pan.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.png.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pry.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.per.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.phl.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pol.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.prt.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pri.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.qat.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rou.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rus.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.rwa.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.wsm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.smr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.stp.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sau.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sen.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.srb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.syc.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sle.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sgp.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sxm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.svk.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.svn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.slb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.som.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.zaf.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ssd.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.esp.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lka.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.kna.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.lca.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.maf.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.vct.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sdn.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.sur.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.swe.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.che.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.syr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tjk.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tza.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tha.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tls.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tgo.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ton.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tto.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tun.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tur.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tkm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tca.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.tuv.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.uga.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ukr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.are.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.gbr.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.usa.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ury.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.uzb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.vut.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.ven.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.vnm.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.vir.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.pse.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.yem.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.zmb.map_or(AnyValue::Null, AnyValue::Float64));
        vals.push(self.zwe.map_or(AnyValue::Null, AnyValue::Float64));

        Row::new(vals)
    }

    /// Read rows for a specific WDI indicator and year range, selecting only the requested
    /// country/region codes. The returned rows are NARROW: they contain exactly
    /// `[indicator_name, indicator_code, year, <countries...>]` in the same order as
    /// provided in `countries`.
    pub async fn read_indicator_countries<'a>(
        conn: Arc<Mutex<Connection>>,
        table: &str,
        indicator_code: &str,
        year_range: (i32, i32),
        countries: &[&str],
    ) -> Result<Vec<Row<'a>>, AppError> {
        // Own all borrowed inputs so they can move into the blocking thread safely.
        let table_owned = table.to_string();
        let indicator_code_owned = indicator_code.to_string();
        let countries_owned: Vec<String> = countries.iter().map(|s| (*s).to_string()).collect();

        tokio::task::spawn_blocking(move || {
            if countries_owned.is_empty() {
                return Ok(Vec::new());
            }

            // Build projection with safe quoting/aliasing
            let mut select_cols: Vec<String> = vec![
                "\"Indicator Name\" AS indicator_name".to_string(),
                "\"Indicator Code\" AS indicator_code".to_string(),
                "CAST(\"Year\" AS BIGINT) AS year".to_string(),
            ];
            for c in &countries_owned {
                let ident = quote_ident(c.as_str());
                let alias = lower_name(c.as_str());
                select_cols.push(format!("CAST({} AS DOUBLE) AS {}", ident, alias));
            }
            let projection = select_cols.join(", ");
            let table_quoted = quote_ident(table_owned.as_str());
            let icode = indicator_code_owned.replace('\'', "''");

            let sql = format!(
                "SELECT {projection} FROM {table} \
                 WHERE \"Indicator Code\" = '{icode}' \
                 AND CAST(\"Year\" AS BIGINT) BETWEEN {start} AND {end} \
                 ORDER BY CAST(\"Year\" AS BIGINT)",
                projection = projection,
                table = table_quoted,
                icode = icode,
                start = year_range.0,
                end = year_range.1
            );

            let conn_guard = conn.lock().expect("duckdb connection mutex poisoned");
            let mut stmt = conn_guard.prepare(sql.as_str())?;
            let mut reader = stmt.query_arrow([])?; // Arrow RecordBatchReader
            let mut out: Vec<Row<'static>> = Vec::new();

            while let Some(batch) = reader.next() {
                let schema = batch.schema();

                // Fixed columns
                let idx_name = schema.index_of("indicator_name").unwrap();
                let idx_code = schema.index_of("indicator_code").unwrap();
                let idx_year = schema.index_of("year").unwrap();

                let a_name = batch
                    .column(idx_name)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let a_code = batch
                    .column(idx_code)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let a_year = batch
                    .column(idx_year)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();

                // Dynamic numeric columns (aliased to lowercase names). Store owned alias to avoid lifetime issues.
                let mut num_arrays: Vec<(String, &Float64Array)> =
                    Vec::with_capacity(countries_owned.len());
                for c in &countries_owned {
                    let alias = lower_name(c.as_str());
                    let col_idx = schema
                        .index_of(&alias)
                        .unwrap_or_else(|_| schema.index_of(c.as_str()).unwrap());
                    let arr = batch
                        .column(col_idx)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap();
                    num_arrays.push((alias, arr));
                }

                for i in 0..batch.num_rows() {
                    // Build a row that matches the caller's expected schema strictly:
                    // indicator_name, indicator_code, year, then the requested countries in order.
                    let mut vals: Vec<AnyValue<'static>> = Vec::with_capacity(3 + num_arrays.len());
                    vals.push(AnyValue::StringOwned(a_name.value(i).to_string().into()));
                    vals.push(AnyValue::StringOwned(a_code.value(i).to_string().into()));
                    vals.push(AnyValue::Int32(a_year.value(i) as i32));

                    for (_alias, arr) in &num_arrays {
                        let v = if arr.is_null(i) {
                            AnyValue::Null
                        } else {
                            AnyValue::Float64(arr.value(i))
                        };
                        vals.push(v);
                    }

                    out.push(Row::new(vals));
                }
            }

            Ok::<Vec<Row>, AppError>(out)
        })
        .await?
    }

    /// Build a Polars schema matching `read_indicator_countries` output for the provided countries.
    pub fn polars_schema_for(countries: &[&str]) -> Schema {
        let mut fields: Vec<Field> = Vec::with_capacity(3 + countries.len());
        fields.push(Field::new("indicator_name".into(), DataType::String));
        fields.push(Field::new("indicator_code".into(), DataType::String));
        fields.push(Field::new("year".into(), DataType::Int32));
        for c in countries {
            fields.push(Field::new(lower_name(c).into(), DataType::Float64));
        }
        Schema::from_iter(fields)
    }
}

impl WdiWide {
    fn set_by_code(&mut self, code: &str, val: Option<f64>) {
        match code {
            // Regions and aggregates
            "afe" => self.afe = val,
            "afw" => self.afw = val,
            "arb" => self.arb = val,
            "css" => self.css = val,
            "ceb" => self.ceb = val,
            "ear" => self.ear = val,
            "eas" => self.eas = val,
            "eap" => self.eap = val,
            "tea" => self.tea = val,
            "emu" => self.emu = val,
            "ecs" => self.ecs = val,
            "eca" => self.eca = val,
            "tec" => self.tec = val,
            "euu" => self.euu = val,
            "fcs" => self.fcs = val,
            "hpc" => self.hpc = val,
            "hic" => self.hic = val,
            "ibd" => self.ibd = val,
            "ibt" => self.ibt = val,
            "idb" => self.idb = val,
            "idx" => self.idx = val,
            "ida" => self.ida = val,
            "lte" => self.lte = val,
            "lcn" => self.lcn = val,
            "lac" => self.lac = val,
            "tla" => self.tla = val,
            "ldc" => self.ldc = val,
            "lmy" => self.lmy = val,
            "lic" => self.lic = val,
            "lmc" => self.lmc = val,
            "mea" => self.mea = val,
            "mna" => self.mna = val,
            "tmn" => self.tmn = val,
            "mic" => self.mic = val,
            "nac" => self.nac = val,
            "inx" => self.inx = val,
            "oed" => self.oed = val,
            "oss" => self.oss = val,
            "pss" => self.pss = val,
            "pst" => self.pst = val,
            "pre" => self.pre = val,
            "sst" => self.sst = val,
            "sas" => self.sas = val,
            "tsa" => self.tsa = val,
            "ssf" => self.ssf = val,
            "ssa" => self.ssa = val,
            "tss" => self.tss = val,
            "umc" => self.umc = val,
            "wld" => self.wld = val,

            // Countries
            "afg" => self.afg = val,
            "alb" => self.alb = val,
            "dza" => self.dza = val,
            "asm" => self.asm = val,
            "and" => self.and = val,
            "ago" => self.ago = val,
            "atg" => self.atg = val,
            "arg" => self.arg = val,
            "arm" => self.arm = val,
            "abw" => self.abw = val,
            "aus" => self.aus = val,
            "aut" => self.aut = val,
            "aze" => self.aze = val,
            "bhs" => self.bhs = val,
            "bhr" => self.bhr = val,
            "bgd" => self.bgd = val,
            "brb" => self.brb = val,
            "blr" => self.blr = val,
            "bel" => self.bel = val,
            "blz" => self.blz = val,
            "ben" => self.ben = val,
            "bmu" => self.bmu = val,
            "btn" => self.btn = val,
            "bol" => self.bol = val,
            "bih" => self.bih = val,
            "bwa" => self.bwa = val,
            "bra" => self.bra = val,
            "vgb" => self.vgb = val,
            "brn" => self.brn = val,
            "bgr" => self.bgr = val,
            "bfa" => self.bfa = val,
            "bdi" => self.bdi = val,
            "cpv" => self.cpv = val,
            "khm" => self.khm = val,
            "cmr" => self.cmr = val,
            "can" => self.can = val,
            "cym" => self.cym = val,
            "caf" => self.caf = val,
            "tcd" => self.tcd = val,
            "chi" => self.chi = val,
            "chl" => self.chl = val,
            "chn" => self.chn = val,
            "col" => self.col = val,
            "com" => self.com = val,
            "cod" => self.cod = val,
            "cog" => self.cog = val,
            "cri" => self.cri = val,
            "civ" => self.civ = val,
            "hrv" => self.hrv = val,
            "cub" => self.cub = val,
            "cuw" => self.cuw = val,
            "cyp" => self.cyp = val,
            "cze" => self.cze = val,
            "dnk" => self.dnk = val,
            "dji" => self.dji = val,
            "dma" => self.dma = val,
            "dom" => self.dom = val,
            "ecu" => self.ecu = val,
            "egy" => self.egy = val,
            "slv" => self.slv = val,
            "gnq" => self.gnq = val,
            "eri" => self.eri = val,
            "est" => self.est = val,
            "swz" => self.swz = val,
            "eth" => self.eth = val,
            "fro" => self.fro = val,
            "fji" => self.fji = val,
            "fin" => self.fin = val,
            "fra" => self.fra = val,
            "pyf" => self.pyf = val,
            "gab" => self.gab = val,
            "gmb" => self.gmb = val,
            "geo" => self.geo = val,
            "deu" => self.deu = val,
            "gha" => self.gha = val,
            "gib" => self.gib = val,
            "grc" => self.grc = val,
            "grl" => self.grl = val,
            "grd" => self.grd = val,
            "gum" => self.gum = val,
            "gtm" => self.gtm = val,
            "gin" => self.gin = val,
            "gnb" => self.gnb = val,
            "guy" => self.guy = val,
            "hti" => self.hti = val,
            "hnd" => self.hnd = val,
            "hkg" => self.hkg = val,
            "hun" => self.hun = val,
            "isl" => self.isl = val,
            "ind" => self.ind = val,
            "idn" => self.idn = val,
            "irn" => self.irn = val,
            "irq" => self.irq = val,
            "irl" => self.irl = val,
            "imn" => self.imn = val,
            "isr" => self.isr = val,
            "ita" => self.ita = val,
            "jam" => self.jam = val,
            "jpn" => self.jpn = val,
            "jor" => self.jor = val,
            "kaz" => self.kaz = val,
            "ken" => self.ken = val,
            "kir" => self.kir = val,
            "prk" => self.prk = val,
            "kor" => self.kor = val,
            "xkx" => self.xkx = val,
            "kwt" => self.kwt = val,
            "kgz" => self.kgz = val,
            "lao" => self.lao = val,
            "lva" => self.lva = val,
            "lbn" => self.lbn = val,
            "lso" => self.lso = val,
            "lbr" => self.lbr = val,
            "lby" => self.lby = val,
            "lie" => self.lie = val,
            "ltu" => self.ltu = val,
            "lux" => self.lux = val,
            "mac" => self.mac = val,
            "mdg" => self.mdg = val,
            "mwi" => self.mwi = val,
            "mys" => self.mys = val,
            "mdv" => self.mdv = val,
            "mli" => self.mli = val,
            "mlt" => self.mlt = val,
            "mhl" => self.mhl = val,
            "mrt" => self.mrt = val,
            "mus" => self.mus = val,
            "mex" => self.mex = val,
            "fsm" => self.fsm = val,
            "mda" => self.mda = val,
            "mco" => self.mco = val,
            "mng" => self.mng = val,
            "mne" => self.mne = val,
            "mar" => self.mar = val,
            "moz" => self.moz = val,
            "mmr" => self.mmr = val,
            "nam" => self.nam = val,
            "nru" => self.nru = val,
            "npl" => self.npl = val,
            "nld" => self.nld = val,
            "ncl" => self.ncl = val,
            "nzl" => self.nzl = val,
            "nic" => self.nic = val,
            "ner" => self.ner = val,
            "nga" => self.nga = val,
            "mkd" => self.mkd = val,
            "mnp" => self.mnp = val,
            "nor" => self.nor = val,
            "omn" => self.omn = val,
            "pak" => self.pak = val,
            "plw" => self.plw = val,
            "pan" => self.pan = val,
            "png" => self.png = val,
            "pry" => self.pry = val,
            "per" => self.per = val,
            "phl" => self.phl = val,
            "pol" => self.pol = val,
            "prt" => self.prt = val,
            "pri" => self.pri = val,
            "qat" => self.qat = val,
            "rou" => self.rou = val,
            "rus" => self.rus = val,
            "rwa" => self.rwa = val,
            "wsm" => self.wsm = val,
            "smr" => self.smr = val,
            "stp" => self.stp = val,
            "sau" => self.sau = val,
            "sen" => self.sen = val,
            "srb" => self.srb = val,
            "syc" => self.syc = val,
            "sle" => self.sle = val,
            "sgp" => self.sgp = val,
            "sxm" => self.sxm = val,
            "svk" => self.svk = val,
            "svn" => self.svn = val,
            "slb" => self.slb = val,
            "som" => self.som = val,
            "zaf" => self.zaf = val,
            "ssd" => self.ssd = val,
            "esp" => self.esp = val,
            "lka" => self.lka = val,
            "kna" => self.kna = val,
            "lca" => self.lca = val,
            "maf" => self.maf = val,
            "vct" => self.vct = val,
            "sdn" => self.sdn = val,
            "sur" => self.sur = val,
            "swe" => self.swe = val,
            "che" => self.che = val,
            "syr" => self.syr = val,
            "tjk" => self.tjk = val,
            "tza" => self.tza = val,
            "tha" => self.tha = val,
            "tls" => self.tls = val,
            "tgo" => self.tgo = val,
            "ton" => self.ton = val,
            "tto" => self.tto = val,
            "tun" => self.tun = val,
            "tur" => self.tur = val,
            "tkm" => self.tkm = val,
            "tca" => self.tca = val,
            "tuv" => self.tuv = val,
            "uga" => self.uga = val,
            "ukr" => self.ukr = val,
            "are" => self.are = val,
            "gbr" => self.gbr = val,
            "usa" => self.usa = val,
            "ury" => self.ury = val,
            "uzb" => self.uzb = val,
            "vut" => self.vut = val,
            "ven" => self.ven = val,
            "vnm" => self.vnm = val,
            "vir" => self.vir = val,
            "pse" => self.pse = val,
            "yem" => self.yem = val,
            "zmb" => self.zmb = val,
            "zwe" => self.zwe = val,
            _ => {}
        }
    }
}

fn quote_ident(name: &str) -> String {
    // Quote if not a simple [A-Za-z0-9_]+ identifier.
    if name
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

fn lower_name(code: &str) -> String {
    code.to_ascii_lowercase()
}
