package net.openhft.chronicle.engine;

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.onoes.ExceptionKey;
import net.openhft.chronicle.engine.api.map.MapView;
import net.openhft.chronicle.engine.tree.ChronicleQueueView;
import net.openhft.chronicle.engine.tree.QueueView;
import net.openhft.chronicle.engine.tree.VanillaAssetTree;
import net.openhft.chronicle.network.TCPRegistry;
import net.openhft.chronicle.network.connection.TcpChannelHub;
import net.openhft.chronicle.wire.AbstractMarshallable;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.LockSupport;

import static net.openhft.chronicle.core.Jvm.*;

/**
 * @author Rob Austin.
 */
class StartEngineWithDummyData {


    private static final String NAME = "throughputTest";
    private static VanillaAssetTree TREE1 = EngineInstance.engineMain(1, "single-host-engine.yaml");
    private static VanillaAssetTree TREE2 = EngineInstance.engineMain(2, "single-host-engine.yaml");
    private static String CLUSTER_NAME = EngineInstance.firstClusterName(TREE2);

    public static void main(String[] args) {
        addSampleDataToTree(TREE1);
        LockSupport.park();
    }

    static {

        try {
            //Delete any files from the last run
            Files.deleteIfExists(Paths.get(OS.TARGET, NAME));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static class MarketData2 extends AbstractMarshallable {
        Date date;
        double open;
        double high;
        double low;
        double close;
        double volume;
        double adjClose;

        public MarketData2(Date date, double open, double high, double low, double close,
                           double adjClose, final double v) {
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = v;
            this.adjClose = adjClose;
        }
    }


    public void close() {
        TREE1.close();
        TREE2.close();
        TcpChannelHub.closeAllHubs();
        TCPRegistry.reset();

    }


    public static class Message extends AbstractMarshallable {
        String message;

        public Message(String message) {
            this.message = message;
        }
    }


    private void throughput(int millionsPerMin, boolean warmup, final String cluster) {

        resetExceptionHandlers();
        disableDebugHandler();

        final String uri1 = "/queue/throughput/replicated";

        @NotNull

        ChronicleQueueView qv1 = (ChronicleQueueView) TREE1.acquireQueue(
                uri1, String.class, Message.class, cluster);
        TREE2.acquireQueue(uri1, String.class, Message.class, cluster);

        addSampleDataToTree(TREE2);

    }


    public static void addSampleDataToTree(final VanillaAssetTree tree) {
        addMyNumbers(tree);
        addApplShares(tree);
        addCountryNumerics(tree);
    }

    private static void addCountryNumerics(VanillaAssetTree tree) {
        @NotNull MapView<String, String> mapView = tree.acquireMap("/my/demo", String.class,
                String.class);
        mapView.put("AED", "United Arab Emirates dirham");
        mapView.put("AFN", "Afghan afghani");
        mapView.put("ALL", "Albanian lek");
        mapView.put("AMD", "Armenian dram");
        mapView.put("ANG", "Netherlands Antillean guilder");
        mapView.put("AOA", "Angolan kwanza");
        mapView.put("ARS", "Argentine peso");
        mapView.put("AUD", "Australian dollar");
        mapView.put("AWG", "Aruban florin");
        mapView.put("AZN", "Azerbaijani manat");
        mapView.put("BAM", "Bosnia and Herzegovina convertible mark");
        mapView.put("BBD", "Barbados dollar");
        mapView.put("BDT", "Bangladeshi taka");
        mapView.put("BGN", "Bulgarian lev");
        mapView.put("BHD", "Bahraini dinar");
        mapView.put("BIF", "Burundian franc");
        mapView.put("BMD", "Bermudian dollar");
        mapView.put("BND", "Brunei dollar");
        mapView.put("BOB", "Boliviano");
        mapView.put("BOV", "Bolivian Mvdol (funds code)");
        mapView.put("BRL", "Brazilian real");
        mapView.put("BSD", "Bahamian dollar");
        mapView.put("BTN", "Bhutanese ngultrum");
        mapView.put("BWP", "Botswana pula");
        mapView.put("BYN", "Belarusian ruble");
        mapView.put("BYR", "Belarusian ruble");
        mapView.put("BZD", "Belize dollar");
        mapView.put("CAD", "Canadian dollar");
        mapView.put("CDF", "Congolese franc");
        mapView.put("CHE", "WIR Euro (complementary currency)");
        mapView.put("CHF", "Swiss franc");
        mapView.put("CHW", "WIR Franc (complementary currency)");
        mapView.put("CLF", "Unidad de Fomento (funds code)");
        mapView.put("CLP", "Chilean peso");
        mapView.put("CNY", "Chinese yuan");
        mapView.put("COP", "Colombian peso");
        mapView.put("COU", "Unidad de Valor Real (UVR) (funds code)[7]");
        mapView.put("CRC", "Costa Rican colon");
        mapView.put("CUC", "Cuban convertible peso");
        mapView.put("CUP", "Cuban peso");
        mapView.put("CVE", "Cape Verde escudo");
        mapView.put("CZK", "Czech koruna");
        mapView.put("DJF", "Djiboutian franc");
        mapView.put("DKK", "Danish krone");
        mapView.put("DOP", "Dominican peso");
        mapView.put("DZD", "Algerian dinar");
        mapView.put("EGP", "Egyptian pound");
        mapView.put("ERN", "Eritrean nakfa");
        mapView.put("ETB", "Ethiopian birr");
        mapView.put("EUR", "Euro");
        mapView.put("FJD", "Fiji dollar");
        mapView.put("FKP", "Falkland Islands pound");
        mapView.put("GBP", "Pound sterling");
        mapView.put("GEL", "Georgian lari");
        mapView.put("GHS", "Ghanaian cedi");
        mapView.put("GIP", "Gibraltar pound");
        mapView.put("GMD", "Gambian dalasi");
        mapView.put("GNF", "Guinean franc");
        mapView.put("GTQ", "Guatemalan quetzal");
        mapView.put("GYD", "Guyanese dollar");
        mapView.put("HKD", "Hong Kong dollar");
        mapView.put("HNL", "Honduran lempira");
        mapView.put("HRK", "Croatian kuna");
        mapView.put("HTG", "Haitian gourde");
        mapView.put("HUF", "Hungarian forint");
        mapView.put("IDR", "Indonesian rupiah");
        mapView.put("ILS", "Israeli new shekel");
        mapView.put("INR", "Indian rupee");
        mapView.put("IQD", "Iraqi dinar");
        mapView.put("IRR", "Iranian rial");
        mapView.put("ISK", "Icelandic króna");
        mapView.put("JMD", "Jamaican dollar");
        mapView.put("JOD", "Jordanian dinar");
        mapView.put("JPY", "Japanese yen");
        mapView.put("KES", "Kenyan shilling");
        mapView.put("KGS", "Kyrgyzstani som");
        mapView.put("KHR", "Cambodian riel");
        mapView.put("KMF", "Comoro franc");
        mapView.put("KPW", "North Korean won");
        mapView.put("KRW", "South Korean won");
        mapView.put("KWD", "Kuwaiti dinar");
        mapView.put("KYD", "Cayman Islands dollar");
        mapView.put("KZT", "Kazakhstani tenge");
        mapView.put("LAK", "Lao kip");
        mapView.put("LBP", "Lebanese pound");
        mapView.put("LKR", "Sri Lankan rupee");
        mapView.put("LRD", "Liberian dollar");
        mapView.put("LSL", "Lesotho loti");
        mapView.put("LYD", "Libyan dinar");
        mapView.put("MAD", "Moroccan dirham");
        mapView.put("MDL", "Moldovan leu");
        mapView.put("MGA", "Malagasy ariary");
        mapView.put("MKD", "Macedonian denar");
        mapView.put("MMK", "Myanmar kyat");
        mapView.put("MNT", "Mongolian tögrög");
        mapView.put("MOP", "Macanese pataca");
        mapView.put("MRO", "Mauritanian ouguiya");
        mapView.put("MUR", "Mauritian rupee");
        mapView.put("MVR", "Maldivian rufiyaa");
        mapView.put("MWK", "Malawian kwacha");
        mapView.put("MXN", "Mexican peso");
        mapView.put("MXV", "Mexican Unidad de Inversion (UDI) (funds code)");
        mapView.put("MYR", "Malaysian ringgit");
        mapView.put("MZN", "Mozambican metical");
        mapView.put("NAD", "Namibian dollar");
        mapView.put("NGN", "Nigerian naira");
        mapView.put("NIO", "Nicaraguan córdoba");
        mapView.put("NOK", "Norwegian krone");
        mapView.put("NPR", "Nepalese rupee");
        mapView.put("NZD", "New Zealand dollar");
        mapView.put("OMR", "Omani rial");
        mapView.put("PAB", "Panamanian balboa");
        mapView.put("PEN", "Peruvian Sol");
        mapView.put("PGK", "Papua New Guinean kina");
        mapView.put("PHP", "Philippine peso");
        mapView.put("PKR", "Pakistani rupee");
        mapView.put("PLN", "Polish złoty");
        mapView.put("PYG", "Paraguayan guaraní");
        mapView.put("QAR", "Qatari riyal");
        mapView.put("RON", "Romanian leu");
        mapView.put("RSD", "Serbian dinar");
        mapView.put("RUB", "Russian ruble");
        mapView.put("RWF", "Rwandan franc");
        mapView.put("SAR", "Saudi riyal");
        mapView.put("SBD", "Solomon Islands dollar");
        mapView.put("SCR", "Seychelles rupee");
        mapView.put("SDG", "Sudanese pound");
        mapView.put("SEK", "Swedish krona/kronor");
        mapView.put("SGD", "Singapore dollar");
        mapView.put("SHP", "Saint Helena pound");
        mapView.put("SLL", "Sierra Leonean leone");
        mapView.put("SOS", "Somali shilling");
        mapView.put("SRD", "Surinamese dollar");
        mapView.put("SSP", "South Sudanese pound");
        mapView.put("STD", "São Tomé and Príncipe dobra");
        mapView.put("SVC", "Salvadoran colón");
        mapView.put("SYP", "Syrian pound");
        mapView.put("SZL", "Swazi lilangeni");
        mapView.put("THB", "Thai baht");
        mapView.put("TJS", "Tajikistani somoni");
        mapView.put("TMT", "Turkmenistani manat");
        mapView.put("TND", "Tunisian dinar");
        mapView.put("TOP", "Tongan paʻanga");
        mapView.put("TRY", "Turkish lira");
        mapView.put("TTD", "Trinidad and Tobago dollar");
        mapView.put("TWD", "New Taiwan dollar");
        mapView.put("TZS", "Tanzanian shilling");
        mapView.put("UAH", "Ukrainian hryvnia");
        mapView.put("UGX", "Ugandan shilling");
        mapView.put("USD", "United States dollar");
        mapView.put("UYU", "Uruguayan peso");
        mapView.put("UZS", "Uzbekistan som");
        mapView.put("VEF", "Venezuelan bolívar");
        mapView.put("VND", "Vietnamese dong");
        mapView.put("VUV", "Vanuatu vatu");
        mapView.put("WST", "Samoan tala");
        mapView.put("XAF", "CFA franc BEAC");
        mapView.put("XCD", "East Caribbean dollar");
        mapView.put("XDR", "Special drawing rights");
        mapView.put("XOF", "CFA franc BCEAO");
        mapView.put("XPD", "Palladium (one troy ounce)");
        mapView.put("XPF", "CFP franc (franc Pacifique)");
        mapView.put("XPT", "Platinum (one troy ounce)");
        mapView.put("XSU", "SUCRE");
        mapView.put("XTS", "Code reserved for testing purposes");
        mapView.put("XUA", "ADB Unit of Account");
        mapView.put("XXX", "No currency");
        mapView.put("YER", "Yemeni rial");
        mapView.put("ZAR", "South African rand");
        mapView.put("ZMW", "Zambian kwacha");
        mapView.put("ZWL", "Zimbabwean dollar A/10");
        mapView.put("Code", "Currency");
        mapView.put("AED", "United Arab Emirates dirham");
        mapView.put("AFN", "Afghan afghani");
        mapView.put("ALL", "Albanian lek");
        mapView.put("AMD", "Armenian dram");
        mapView.put("ANG", "Netherlands Antillean guilder");
        mapView.put("AOA", "Angolan kwanza");
        mapView.put("ARS", "Argentine peso");
        mapView.put("AUD", "Australian dollar");
        mapView.put("AWG", "Aruban florin");
        mapView.put("AZN", "Azerbaijani manat");
        mapView.put("BAM", "Bosnia and Herzegovina convertible mark");
        mapView.put("BBD", "Barbados dollar");
        mapView.put("BDT", "Bangladeshi taka");
        mapView.put("BGN", "Bulgarian lev");
        mapView.put("BHD", "Bahraini dinar");
        mapView.put("BIF", "Burundian franc");
        mapView.put("BMD", "Bermudian dollar");
        mapView.put("BND", "Brunei dollar");
        mapView.put("BOB", "Boliviano");
        mapView.put("BOV", "Bolivian Mvdol (funds code)");
        mapView.put("BRL", "Brazilian real");
        mapView.put("BSD", "Bahamian dollar");
        mapView.put("BTN", "Bhutanese ngultrum");
        mapView.put("BWP", "Botswana pula");
        mapView.put("BYN", "Belarusian ruble");
        mapView.put("BYR", "Belarusian ruble");
        mapView.put("BZD", "Belize dollar");
        mapView.put("CAD", "Canadian dollar");
        mapView.put("CDF", "Congolese franc");
        mapView.put("CHF", "Swiss franc");
        mapView.put("CLP", "Chilean peso");
        mapView.put("CNY", "Chinese yuan");
        mapView.put("COP", "Colombian peso");
        mapView.put("CRC", "Costa Rican colon");
        mapView.put("CUC", "Cuban convertible peso");
        mapView.put("CUP", "Cuban peso");
        mapView.put("CVE", "Cape Verde escudo");
        mapView.put("CZK", "Czech koruna");
        mapView.put("DJF", "Djiboutian franc");
        mapView.put("DKK", "Danish krone");
        mapView.put("DOP", "Dominican peso");
        mapView.put("DZD", "Algerian dinar");
        mapView.put("EGP", "Egyptian pound");
        mapView.put("ERN", "Eritrean nakfa");
        mapView.put("ETB", "Ethiopian birr");
        mapView.put("EUR", "Euro");
        mapView.put("FJD", "Fiji dollar");
        mapView.put("FKP", "Falkland Islands pound");
        mapView.put("GBP", "Pound sterling");
        mapView.put("GEL", "Georgian lari");
        mapView.put("GHS", "Ghanaian cedi");
        mapView.put("GIP", "Gibraltar pound");
        mapView.put("GMD", "Gambian dalasi");
        mapView.put("GNF", "Guinean franc");
        mapView.put("GTQ", "Guatemalan quetzal");
        mapView.put("GYD", "Guyanese dollar");
        mapView.put("HKD", "Hong Kong dollar");
        mapView.put("HNL", "Honduran lempira");
        mapView.put("HRK", "Croatian kuna");
        mapView.put("HTG", "Haitian gourde");
        mapView.put("HUF", "Hungarian forint");
        mapView.put("IDR", "Indonesian rupiah");
        mapView.put("ILS", "Israeli new shekel");
        mapView.put("INR", "Indian rupee");
        mapView.put("IQD", "Iraqi dinar");
        mapView.put("IRR", "Iranian rial");
        mapView.put("ISK", "Icelandic króna");
        mapView.put("JMD", "Jamaican dollar");
        mapView.put("JOD", "Jordanian dinar");
        mapView.put("JPY", "Japanese yen");
        mapView.put("KES", "Kenyan shilling");
        mapView.put("KGS", "Kyrgyzstani som");
        mapView.put("KHR", "Cambodian riel");
        mapView.put("KMF", "Comoro franc");
        mapView.put("KPW", "North Korean won");
        mapView.put("KRW", "South Korean won");
        mapView.put("KWD", "Kuwaiti dinar");
        mapView.put("KYD", "Cayman Islands dollar");
        mapView.put("KZT", "Kazakhstani tenge");
        mapView.put("LAK", "Lao kip");
        mapView.put("LBP", "Lebanese pound");
        mapView.put("LKR", "Sri Lankan rupee");
        mapView.put("LRD", "Liberian dollar");
        mapView.put("LSL", "Lesotho loti");
        mapView.put("LYD", "Libyan dinar");
        mapView.put("MAD", "Moroccan dirham");
        mapView.put("MDL", "Moldovan leu");
        mapView.put("MGA", "Malagasy ariary");
        mapView.put("MKD", "Macedonian denar");
        mapView.put("MMK", "Myanmar kyat");
        mapView.put("MNT", "Mongolian tögrög");
        mapView.put("MOP", "Macanese pataca");
        mapView.put("MRO", "Mauritanian ouguiya");
        mapView.put("MUR", "Mauritian rupee");
        mapView.put("MVR", "Maldivian rufiyaa");
        mapView.put("MWK", "Malawian kwacha");
        mapView.put("MXN", "Mexican peso");
        mapView.put("MXV", "Mexican Unidad de Inversion (UDI) (funds code)");
        mapView.put("MYR", "Malaysian ringgit");
        mapView.put("MZN", "Mozambican metical");
        mapView.put("NAD", "Namibian dollar");
        mapView.put("NGN", "Nigerian naira");
        mapView.put("NIO", "Nicaraguan córdoba");
        mapView.put("NOK", "Norwegian krone");
        mapView.put("NPR", "Nepalese rupee");
        mapView.put("NZD", "New Zealand dollar");
        mapView.put("OMR", "Omani rial");
        mapView.put("PAB", "Panamanian balboa");
        mapView.put("PEN", "Peruvian Sol");
        mapView.put("PGK", "Papua New Guinean kina");
        mapView.put("PHP", "Philippine peso");
        mapView.put("PKR", "Pakistani rupee");
        mapView.put("PLN", "Polish złoty");
        mapView.put("PYG", "Paraguayan guaraní");
        mapView.put("QAR", "Qatari riyal");
        mapView.put("RON", "Romanian leu");
        mapView.put("RSD", "Serbian dinar");
        mapView.put("RUB", "Russian ruble");
        mapView.put("RWF", "Rwandan franc");
        mapView.put("SAR", "Saudi riyal");
        mapView.put("SBD", "Solomon Islands dollar");
        mapView.put("SCR", "Seychelles rupee");
        mapView.put("SDG", "Sudanese pound");
        mapView.put("SEK", "Swedish krona/kronor");
        mapView.put("SGD", "Singapore dollar");
        mapView.put("SHP", "Saint Helena pound");
        mapView.put("SLL", "Sierra Leonean leone");
        mapView.put("SOS", "Somali shilling");
        mapView.put("SRD", "Surinamese dollar");
        mapView.put("SSP", "South Sudanese pound");
        mapView.put("STD", "São Tomé and Príncipe dobra");
        mapView.put("SVC", "Salvadoran colón");
        mapView.put("SYP", "Syrian pound");
        mapView.put("SZL", "Swazi lilangeni");
        mapView.put("THB", "Thai baht");
        mapView.put("TJS", "Tajikistani somoni");
        mapView.put("TMT", "Turkmenistani manat");
        mapView.put("TND", "Tunisian dinar");
        mapView.put("TOP", "Tongan paʻanga");
        mapView.put("TRY", "Turkish lira");
        mapView.put("TTD", "Trinidad and Tobago dollar");
        mapView.put("TWD", "New Taiwan dollar");
        mapView.put("TZS", "Tanzanian shilling");
        mapView.put("UAH", "Ukrainian hryvnia");
        mapView.put("UGX", "Ugandan shilling");
        mapView.put("USD", "United States dollar");
        mapView.put("UYI", "Uruguay Peso en Unidades Indexadas (URUIURUI) (funds code)");
        mapView.put("UYU", "Uruguayan peso");
        mapView.put("UZS", "Uzbekistan som");
        mapView.put("VEF", "Venezuelan bolívar");
        mapView.put("VND", "Vietnamese dong");
        mapView.put("VUV", "Vanuatu vatu");
        mapView.put("WST", "Samoan tala");
        mapView.put("XAF", "CFA franc BEAC");
        mapView.put("XAG", "Silver (one troy ounce)");
    }

    private static void addApplShares(VanillaAssetTree tree) {

        @NotNull SimpleDateFormat sd = new SimpleDateFormat("dd MMM yyyy");

        QueueView<String, MarketData2> q = tree.acquireQueue
                ("/queue/shares/APPL", String.class, MarketData2.class, CLUSTER_NAME);

        try {
            q.publishAndIndex("", new MarketData2(sd.parse("7 Oct 2016"), 114.31, 114.56, 113.51, 114.06, 114.06, 24358400L));
            q.publishAndIndex("", new MarketData2(sd.parse("6 Oct 2016"), 113.70, 114.34, 113.13, 113.89, 113.89, 28779300L));
            q.publishAndIndex("", new MarketData2(sd.parse("5 Oct 2016"), 113.40, 113.66, 112.69, 113.05, 113.05, 21453100L));
            q.publishAndIndex("", new MarketData2(sd.parse("4 Oct 2016"), 113.06, 114.31, 112.63, 113.00, 113.00, 29736800L));
            q.publishAndIndex("", new MarketData2(sd.parse("3 Oct 2016"), 112.71, 113.05, 112.28, 112.52, 112.52, 21701800L));
            q.publishAndIndex("", new MarketData2(sd.parse("30 Sep 2016"), 112.46, 113.37, 111.80, 113.05, 113.05, 36379100L));
            q.publishAndIndex("", new MarketData2(sd.parse("29 Sep 2016"), 113.16, 113.80, 111.80, 112.18, 112.18, 35887000L));
            q.publishAndIndex("", new MarketData2(sd.parse("28 Sep 2016"), 113.69, 114.64, 113.43, 113.95, 113.95, 29641100L));
            q.publishAndIndex("", new MarketData2(sd.parse("27 Sep 2016"), 113.00, 113.18, 112.34, 113.09, 113.09, 24607400L));
            q.publishAndIndex("", new MarketData2(sd.parse("26 Sep 2016"), 111.64, 113.39, 111.55, 112.88, 112.88, 29869400L));
            q.publishAndIndex("", new MarketData2(sd.parse("23 Sep 2016"), 114.42, 114.79, 111.55, 112.71, 112.71, 52481200L));
            q.publishAndIndex("", new MarketData2(sd.parse("22 Sep 2016"), 114.35, 114.94, 114.00, 114.62, 114.62, 31074000L));
            q.publishAndIndex("", new MarketData2(sd.parse("21 Sep 2016"), 113.85, 113.99, 112.44, 113.55, 113.55, 36003200L));
            q.publishAndIndex("", new MarketData2(sd.parse("20 Sep 2016"), 113.05, 114.12, 112.51, 113.57, 113.57, 34514300L));
            q.publishAndIndex("", new MarketData2(sd.parse("19 Sep 2016"), 115.19, 116.18, 113.25, 113.58, 113.58, 47023000L));
            q.publishAndIndex("", new MarketData2(sd.parse("16 Sep 2016"), 115.12, 116.13, 114.04, 114.92, 114.92, 79886900L));
            q.publishAndIndex("", new MarketData2(sd.parse("15 Sep 2016"), 113.86, 115.73, 113.49, 115.57, 115.57, 89983600L));
            q.publishAndIndex("", new MarketData2(sd.parse("14 Sep 2016"), 108.73, 113.03, 108.60, 111.77, 111.77, 110888700L));
            q.publishAndIndex("", new MarketData2(sd.parse("13 Sep 2016"), 107.51, 108.79, 107.24, 107.95, 107.95, 62176200L));
            q.publishAndIndex("", new MarketData2(sd.parse("12 Sep 2016"), 102.65, 105.72, 102.53, 105.44, 105.44, 45292800L));
            q.publishAndIndex("", new MarketData2(sd.parse("9 Sep 2016"), 104.64, 105.72, 103.13, 103.13, 103.13, 46557000L));
            q.publishAndIndex("", new MarketData2(sd.parse("8 Sep 2016"), 107.25, 107.27, 105.24, 105.52, 105.52, 53002000L));
            q.publishAndIndex("", new MarketData2(sd.parse("7 Sep 2016"), 107.83, 108.76, 107.07, 108.36, 108.36, 42364300L));
            q.publishAndIndex("", new MarketData2(sd.parse("6 Sep 2016"), 107.90, 108.30, 107.51, 107.70, 107.70, 26880400L));
            q.publishAndIndex("", new MarketData2(sd.parse("2 Sep 2016"), 107.70, 108.00, 106.82, 107.73, 107.73, 26802500L));
            q.publishAndIndex("", new MarketData2(sd.parse("1 Sep 2016"), 106.14, 106.80, 105.62, 106.73, 106.73, 26701500L));
            q.publishAndIndex("", new MarketData2(sd.parse("31 Aug 2016"), 105.66, 106.57, 105.64, 106.10, 106.10, 29662400L));
            q.publishAndIndex("", new MarketData2(sd.parse("30 Aug 2016"), 105.80, 106.50, 105.50, 106.00, 106.00, 24863900L));
            q.publishAndIndex("", new MarketData2(sd.parse("29 Aug 2016"), 106.62, 107.44, 106.29, 106.82, 106.82, 24970300L));
            q.publishAndIndex("", new MarketData2(sd.parse("26 Aug 2016"), 107.41, 107.95, 106.31, 106.94, 106.94, 27766300L));
            q.publishAndIndex("", new MarketData2(sd.parse("25 Aug 2016"), 107.39, 107.88, 106.68, 107.57, 107.57, 25086200L));
            q.publishAndIndex("", new MarketData2(sd.parse("24 Aug 2016"), 108.57, 108.75, 107.68, 108.03, 108.03, 23675100L));
            q.publishAndIndex("", new MarketData2(sd.parse("23 Aug 2016"), 108.59, 109.32, 108.53, 108.85, 108.85, 21257700L));
            q.publishAndIndex("", new MarketData2(sd.parse("22 Aug 2016"), 108.86, 109.10, 107.85, 108.51, 108.51, 25820200L));
            q.publishAndIndex("", new MarketData2(sd.parse("19 Aug 2016"), 108.77, 109.69, 108.36, 109.36, 109.36, 25368100L));
            q.publishAndIndex("", new MarketData2(sd.parse("18 Aug 2016"), 109.23, 109.60, 109.02, 109.08, 109.08, 21984700L));
            q.publishAndIndex("", new MarketData2(sd.parse("17 Aug 2016"), 109.10, 109.37, 108.34, 109.22, 109.22, 25356000L));
            q.publishAndIndex("", new MarketData2(sd.parse("16 Aug 2016"), 109.63, 110.23, 109.21, 109.38, 109.38, 33794400L));
            q.publishAndIndex("", new MarketData2(sd.parse("15 Aug 2016"), 108.14, 109.54, 108.08, 109.48, 109.48, 25868200L));
            q.publishAndIndex("", new MarketData2(sd.parse("12 Aug 2016"), 107.78, 108.44, 107.78, 108.18, 108.18, 18660400L));
            q.publishAndIndex("", new MarketData2(sd.parse("11 Aug 2016"), 108.52, 108.93, 107.85, 107.93, 107.93, 27484500L));
            q.publishAndIndex("", new MarketData2(sd.parse("10 Aug 2016"), 108.71, 108.90, 107.76, 108.00, 108.00, 24008500L));
            q.publishAndIndex("", new MarketData2(sd.parse("9 Aug 2016"), 108.23, 108.94, 108.01, 108.81, 108.81, 26315200L));
            q.publishAndIndex("", new MarketData2(sd.parse("8 Aug 2016"), 107.52, 108.37, 107.16, 108.37, 108.37, 28037200L));
            q.publishAndIndex("", new MarketData2(sd.parse("5 Aug 2016"), 106.27, 107.65, 106.18, 107.48, 107.48, 40553400L));
            q.publishAndIndex("", new MarketData2(sd.parse("4 Aug 2016"), 105.58, 106.00, 105.28, 105.87, 105.87, 27408700L));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MarketData extends AbstractMarshallable {
        double open;
        double high;
        double low;
        double close;
        double volume;
        double adjClose;

        public MarketData(double open, double high, double low, double close,
                          double adjClose, final double v) {
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = v;
            this.adjClose = adjClose;
        }
    }

    private static void addMyNumbers(VanillaAssetTree tree) {
        @NotNull MapView<Integer, Double> intView = tree.acquireMap(
                "/my/numbers",
                Integer.class,
                Double.class);

        for (int i = 0; i < 100; i++) {
            intView.put(i, (double) i);
        }

        @NotNull
        SimpleDateFormat sd = new SimpleDateFormat("dd MMM yyyy");

        @NotNull
        MapView<Date, MarketData> map = tree.acquireMap("/shares/APPL",
                Date.class,
                MarketData.class);

        try {
            map.put(sd.parse("7 Oct 2016"), new MarketData(114.31, 114.56, 113.51, 114.06, 114.06, 24358400L));
            map.put(sd.parse("6 Oct 2016"), new MarketData(113.70, 114.34, 113.13, 113.89, 113.89, 28779300L));
            map.put(sd.parse("5 Oct 2016"), new MarketData(113.40, 113.66, 112.69, 113.05, 113.05, 21453100L));
            map.put(sd.parse("4 Oct 2016"), new MarketData(113.06, 114.31, 112.63, 113.00, 113.00, 29736800L));
            map.put(sd.parse("3 Oct 2016"), new MarketData(112.71, 113.05, 112.28, 112.52, 112.52, 21701800L));
            map.put(sd.parse("30 Sep 2016"), new MarketData(112.46, 113.37, 111.80, 113.05, 113.05, 36379100L));
            map.put(sd.parse("29 Sep 2016"), new MarketData(113.16, 113.80, 111.80, 112.18, 112.18, 35887000L));
            map.put(sd.parse("28 Sep 2016"), new MarketData(113.69, 114.64, 113.43, 113.95, 113.95, 29641100L));
            map.put(sd.parse("27 Sep 2016"), new MarketData(113.00, 113.18, 112.34, 113.09, 113.09, 24607400L));
            map.put(sd.parse("26 Sep 2016"), new MarketData(111.64, 113.39, 111.55, 112.88, 112.88, 29869400L));
            map.put(sd.parse("23 Sep 2016"), new MarketData(114.42, 114.79, 111.55, 112.71, 112.71, 52481200L));
            map.put(sd.parse("22 Sep 2016"), new MarketData(114.35, 114.94, 114.00, 114.62, 114.62, 31074000L));
            map.put(sd.parse("21 Sep 2016"), new MarketData(113.85, 113.99, 112.44, 113.55, 113.55, 36003200L));
            map.put(sd.parse("20 Sep 2016"), new MarketData(113.05, 114.12, 112.51, 113.57, 113.57, 34514300L));
            map.put(sd.parse("19 Sep 2016"), new MarketData(115.19, 116.18, 113.25, 113.58, 113.58, 47023000L));
            map.put(sd.parse("16 Sep 2016"), new MarketData(115.12, 116.13, 114.04, 114.92, 114.92, 79886900L));
            map.put(sd.parse("15 Sep 2016"), new MarketData(113.86, 115.73, 113.49, 115.57, 115.57, 89983600L));
            map.put(sd.parse("14 Sep 2016"), new MarketData(108.73, 113.03, 108.60, 111.77, 111.77, 110888700L));
            map.put(sd.parse("13 Sep 2016"), new MarketData(107.51, 108.79, 107.24, 107.95, 107.95, 62176200L));
            map.put(sd.parse("12 Sep 2016"), new MarketData(102.65, 105.72, 102.53, 105.44, 105.44, 45292800L));
            map.put(sd.parse("9 Sep 2016"), new MarketData(104.64, 105.72, 103.13, 103.13, 103.13, 46557000L));
            map.put(sd.parse("8 Sep 2016"), new MarketData(107.25, 107.27, 105.24, 105.52, 105.52, 53002000L));
            map.put(sd.parse("7 Sep 2016"), new MarketData(107.83, 108.76, 107.07, 108.36, 108.36, 42364300L));
            map.put(sd.parse("6 Sep 2016"), new MarketData(107.90, 108.30, 107.51, 107.70, 107.70, 26880400L));
            map.put(sd.parse("2 Sep 2016"), new MarketData(107.70, 108.00, 106.82, 107.73, 107.73, 26802500L));
            map.put(sd.parse("1 Sep 2016"), new MarketData(106.14, 106.80, 105.62, 106.73, 106.73, 26701500L));
            map.put(sd.parse("31 Aug 2016"), new MarketData(105.66, 106.57, 105.64, 106.10, 106.10, 29662400L));
            map.put(sd.parse("30 Aug 2016"), new MarketData(105.80, 106.50, 105.50, 106.00, 106.00, 24863900L));
            map.put(sd.parse("29 Aug 2016"), new MarketData(106.62, 107.44, 106.29, 106.82, 106.82, 24970300L));
            map.put(sd.parse("26 Aug 2016"), new MarketData(107.41, 107.95, 106.31, 106.94, 106.94, 27766300L));
            map.put(sd.parse("25 Aug 2016"), new MarketData(107.39, 107.88, 106.68, 107.57, 107.57, 25086200L));
            map.put(sd.parse("24 Aug 2016"), new MarketData(108.57, 108.75, 107.68, 108.03, 108.03, 23675100L));
            map.put(sd.parse("23 Aug 2016"), new MarketData(108.59, 109.32, 108.53, 108.85, 108.85, 21257700L));
            map.put(sd.parse("22 Aug 2016"), new MarketData(108.86, 109.10, 107.85, 108.51, 108.51, 25820200L));
            map.put(sd.parse("19 Aug 2016"), new MarketData(108.77, 109.69, 108.36, 109.36, 109.36, 25368100L));
            map.put(sd.parse("18 Aug 2016"), new MarketData(109.23, 109.60, 109.02, 109.08, 109.08, 21984700L));
            map.put(sd.parse("17 Aug 2016"), new MarketData(109.10, 109.37, 108.34, 109.22, 109.22, 25356000L));
            map.put(sd.parse("16 Aug 2016"), new MarketData(109.63, 110.23, 109.21, 109.38, 109.38, 33794400L));
            map.put(sd.parse("15 Aug 2016"), new MarketData(108.14, 109.54, 108.08, 109.48, 109.48, 25868200L));
            map.put(sd.parse("12 Aug 2016"), new MarketData(107.78, 108.44, 107.78, 108.18, 108.18, 18660400L));
            map.put(sd.parse("11 Aug 2016"), new MarketData(108.52, 108.93, 107.85, 107.93, 107.93, 27484500L));
            map.put(sd.parse("10 Aug 2016"), new MarketData(108.71, 108.90, 107.76, 108.00, 108.00, 24008500L));
            map.put(sd.parse("9 Aug 2016"), new MarketData(108.23, 108.94, 108.01, 108.81, 108.81, 26315200L));
            map.put(sd.parse("8 Aug 2016"), new MarketData(107.52, 108.37, 107.16, 108.37, 108.37, 28037200L));
            map.put(sd.parse("5 Aug 2016"), new MarketData(106.27, 107.65, 106.18, 107.48, 107.48, 40553400L));
            map.put(sd.parse("4 Aug 2016"), new MarketData(105.58, 106.00, 105.28, 105.87, 105.87, 27408700L));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private boolean runThroughput() {
        try {

            throughput(0, false, CLUSTER_NAME);
        } catch (Throwable e) {
            e.printStackTrace();
            Map<ExceptionKey, Integer> ex = new HashMap<ExceptionKey, Integer>();
            dumpException(ex);
            System.out.println(ex);
        }
        return false;
    }

}

