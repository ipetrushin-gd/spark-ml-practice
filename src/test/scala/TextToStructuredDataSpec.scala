import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.FunSpec
import TextToStructuredData.getStructuredDataFromText

//TODO: switch to more concise and powerful spark-testing-base
class TextToStructuredDataSpec extends FunSpec with SparkSessionCreator with DatasetComparer {
  import spark.implicits._

  it("Simple general check") {
    val source = List(
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, true)",
      "List($btc #bitcoin #cryptolife #cryptofamily Good morning, Bitcoin has good support now, he might not instantly jump up,… https://t.co/ok8vMxuE17, en, false)",
      "List(Earn bitcoin on a daily basis!\n1. Follow @slidecoin\n2. Complete instructions in pinned tweet, en, false)",
      "List(RT @nodepower_eu: Great YouTube video review about #Node made by Getting Started with Crypto channel.\n\nThank you for such a profound ana…, en, true)",
      "List(RT @Digitalnaiv: Fundstück: Smart Package: #Blockchain Patent von #Walmart -  via @CIOKurator  https://t.co/pt18IRsjee #Retail #Handel, de, true)",
      "List(#Kepler is one of bigger #AI #ICO still active (link: https://t.co/aUXxAzChbr) https://t.co/gE5c4A4ID8, en, false)",
      "List(@evenjangle ATMS is the perfect cryptocurrency., en, false)",
      "List(.@unit4@unit42_intel はここ6ヵ月で、#仮想通貨 の #マイニング を最終目的とした攻撃活動数の大幅増加を観察。何が攻撃者からの大幅な変化を促進し、業界の著しい動向を生み出しているのでしょうか? 最も多く#マイ… https://t.co/qv2hEfGFA7, ja, false)",
      "List(RT @eGoldgg: If you need to understand the advantages that Blockchain technology can bring to the eSports betting sector - watch Mario Ovch…, en, true)",
      "List(ネムコイン(XEM)とは？特徴・仕組み・今後について徹底解説！  仮想通貨メディアBTC Feedビットコインフィード https://t.co/qSPi7jZxAk, ja, false)",
      "List(9 Stocks to Buy Instead of Bitcoin | InvestorPlace https://t.co/UlJ2GuglpE #markets… https://t.co/KdmrRuJple, en, false)",
      "List(RT @Kora_Network: #Kora was featured on the Crypto Knights Podcast! Check it out. #financialinclusion #blockchain4good\n\nhttps://t.co/2SBwz…, en, true)"
    ).toDS

    val actual = getStructuredDataFromText(source)

    val expected = Seq(
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","en",1,0.toLong),
      ("$btc #bitcoin #cryptolife #cryptofamily Good morning, Bitcoin has good support now, he might not instantly jump up,… https://t.co/ok8vMxuE17","en",0,1.toLong),
      ("Earn bitcoin on a daily basis!\n1. Follow @slidecoin\n2. Complete instructions in pinned tweet","en",0,2.toLong),
      ("RT @nodepower_eu: Great YouTube video review about #Node made by Getting Started with Crypto channel.\n\nThank you for such a profound ana…","en",1,3.toLong),
      ("RT @Digitalnaiv: Fundstück: Smart Package: #Blockchain Patent von #Walmart -  via @CIOKurator  https://t.co/pt18IRsjee #Retail #Handel","de",1,4.toLong),
      ("#Kepler is one of bigger #AI #ICO still active (link: https://t.co/aUXxAzChbr) https://t.co/gE5c4A4ID8","en",0,5.toLong),
      ("@evenjangle ATMS is the perfect cryptocurrency.","en",0,6.toLong),
      (".@unit4@unit42_intel はここ6ヵ月で、#仮想通貨 の #マイニング を最終目的とした攻撃活動数の大幅増加を観察。何が攻撃者からの大幅な変化を促進し、業界の著しい動向を生み出しているのでしょうか? 最も多く#マイ… https://t.co/qv2hEfGFA7","ja",0,7.toLong),
      ("RT @eGoldgg: If you need to understand the advantages that Blockchain technology can bring to the eSports betting sector - watch Mario Ovch…","en",1,8.toLong),
      ("ネムコイン(XEM)とは？特徴・仕組み・今後について徹底解説！  仮想通貨メディアBTC Feedビットコインフィード https://t.co/qSPi7jZxAk","ja",0,9.toLong),
      ("9 Stocks to Buy Instead of Bitcoin | InvestorPlace https://t.co/UlJ2GuglpE #markets… https://t.co/KdmrRuJple","en",0,10.toLong),
      ("RT @Kora_Network: #Kora was featured on the Crypto Knights Podcast! Check it out. #financialinclusion #blockchain4good\n\nhttps://t.co/2SBwz…","en",1,11.toLong)
    ).toDF("text", "language", "label", "id")

    assertSmallDataFrameEquality(actual, expected)
  }

  it("IsRetweet check") {
    val source = List(
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, true)",
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, false)"
    ).toDS

    val actual = getStructuredDataFromText(source)

    val expected = Seq(
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","en",1,0.toLong),
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","en",0,1.toLong)
    ).toDF("text", "language", "label", "id")

    assertSmallDataFrameEquality(actual, expected)
  }

  it("Language check") {
    val source = List(
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, true)",
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, de, true)",
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, jp, true)",
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, fr, true)",
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, ru, false)"
    ).toDS

    val actual = getStructuredDataFromText(source)

    val expected = Seq(
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","en",1,0.toLong),
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","de",1,1.toLong),
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","jp",1,2.toLong),
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","fr",1,3.toLong),
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","ru",0,4.toLong)
    ).toDF("text", "language", "label", "id")

    assertSmallDataFrameEquality(actual, expected)
  }

  it("New lines check") {
    val source = List(
      """List(
        |RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, true)""".stripMargin,
      """List($btc #bitcoin #cryptolife
        |
        |#cryptofamily Good morning, Bitcoin has good support now,
        |he might not instantly jump up,…
        |https://t.co/ok8vMxuE17, en, false)""".stripMargin,
      """List(Earn bitcoin on a daily basis!
        |1. Follow @slidecoin
        |2. Complete instructions in pinned tweet, en, false)""".stripMargin
    ).toDS

    val actual = getStructuredDataFromText(source)

    val expected = Seq(
      ("\nRT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","en",1,0.toLong),
      ("$btc #bitcoin #cryptolife\n\n#cryptofamily Good morning, Bitcoin has good support now,\nhe might not instantly jump up,…\nhttps://t.co/ok8vMxuE17","en",0,1.toLong),
      ("Earn bitcoin on a daily basis!\n1. Follow @slidecoin\n2. Complete instructions in pinned tweet","en",0,2.toLong)
    ).toDF("text", "language", "label", "id")

    assertSmallDataFrameEquality(actual, expected)
  }

  it("Unknown language check") {
    val source = List(
      "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, und, true)"
    ).toDS

    val actual = getStructuredDataFromText(source)

    val expected = Seq(
      ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4","??",1,0.toLong)
    ).toDF("text", "language", "label", "id")

    assertSmallDataFrameEquality(actual, expected)
  }

  it("Empty tweet check") {
    val source = List(
      "List(, und, true)"
    ).toDS

    val actual = getStructuredDataFromText(source)

    val expected = Seq(
      ("","??",1,0.toLong)
    ).toDF("text", "language", "label", "id")

    assertSmallDataFrameEquality(actual, expected)
  }
}
