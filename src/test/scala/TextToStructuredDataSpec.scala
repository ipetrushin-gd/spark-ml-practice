import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.scalatest.FunSpec
import TextToStructuredData.getStructuredDataFromText

//TODO: switch to more concise and powerful spark-testing-base
class TextToStructuredDataSpec extends FunSpec with SparkSessionCreator with DatasetComparer {
  import spark.implicits._

  describe("A tweets") {
    describe("when tweets are real") {
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
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "en", 1),
        ("$btc #bitcoin #cryptolife #cryptofamily Good morning, Bitcoin has good support now, he might not instantly jump up,… https://t.co/ok8vMxuE17", "en", 0),
        ("Earn bitcoin on a daily basis!\n1. Follow @slidecoin\n2. Complete instructions in pinned tweet", "en", 0),
        ("RT @nodepower_eu: Great YouTube video review about #Node made by Getting Started with Crypto channel.\n\nThank you for such a profound ana…", "en", 1),
        ("RT @Digitalnaiv: Fundstück: Smart Package: #Blockchain Patent von #Walmart -  via @CIOKurator  https://t.co/pt18IRsjee #Retail #Handel", "de", 1),
        ("#Kepler is one of bigger #AI #ICO still active (link: https://t.co/aUXxAzChbr) https://t.co/gE5c4A4ID8", "en", 0),
        ("@evenjangle ATMS is the perfect cryptocurrency.", "en", 0),
        (".@unit4@unit42_intel はここ6ヵ月で、#仮想通貨 の #マイニング を最終目的とした攻撃活動数の大幅増加を観察。何が攻撃者からの大幅な変化を促進し、業界の著しい動向を生み出しているのでしょうか? 最も多く#マイ… https://t.co/qv2hEfGFA7", "ja", 0),
        ("RT @eGoldgg: If you need to understand the advantages that Blockchain technology can bring to the eSports betting sector - watch Mario Ovch…", "en", 1),
        ("ネムコイン(XEM)とは？特徴・仕組み・今後について徹底解説！  仮想通貨メディアBTC Feedビットコインフィード https://t.co/qSPi7jZxAk", "ja", 0),
        ("9 Stocks to Buy Instead of Bitcoin | InvestorPlace https://t.co/UlJ2GuglpE #markets… https://t.co/KdmrRuJple", "en", 0),
        ("RT @Kora_Network: #Kora was featured on the Crypto Knights Podcast! Check it out. #financialinclusion #blockchain4good\n\nhttps://t.co/2SBwz…", "en", 1)
      ).toDF("text", "language", "label")

      it("should be processed normally") {
        assertSmallDataFrameEquality(actual, expected)
      }
    }

    describe("with isRetweet field") {
      val source = List(
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, true)",
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, false)"
      ).toDS

      val actual = getStructuredDataFromText(source)

      val expected = Seq(
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "en", 1),
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "en", 0)
      ).toDF("text", "language", "label")

      it("true should be replaced with 1, false - with 0") {
        assertSmallDataFrameEquality(actual, expected)
      }
    }

    describe("with language field") {
      val source = List(
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, en, true)",
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, de, true)",
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, jp, true)",
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, fr, true)",
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, ru, false)"
      ).toDS

      val actual = getStructuredDataFromText(source)

      val expected = Seq(
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "en", 1),
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "de", 1),
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "jp", 1),
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "fr", 1),
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "ru", 0)
      ).toDF("text", "language", "label")

      it("language should be parsed as separate 2-letters string") {
        assertSmallDataFrameEquality(actual, expected)
      }
    }

    describe(" with new-line symbols in text") {
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
        ("\nRT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "en", 1),
        ("$btc #bitcoin #cryptolife\n\n#cryptofamily Good morning, Bitcoin has good support now,\nhe might not instantly jump up,…\nhttps://t.co/ok8vMxuE17", "en", 0),
        ("Earn bitcoin on a daily basis!\n1. Follow @slidecoin\n2. Complete instructions in pinned tweet", "en", 0)
      ).toDF("text", "language", "label")

      it("should be processed in correct way") {
        assertSmallDataFrameEquality(actual, expected)
      }
    }

    describe("with 'und' as language marker") {
      val source = List(
        "List(RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4, und, true)"
      ).toDS

      val actual = getStructuredDataFromText(source)

      val expected = Seq(
        ("RT @emy_wng: Thanks Bloomberg for featuring @Ripple and #MoneyTap! https://t.co/WDuJnD1tq4", "??", 1)
      ).toDF("text", "language", "label")

      it("language should be parsed as '??' (2 question marks)") {
        assertSmallDataFrameEquality(actual, expected)
      }
    }

    describe("without text") {
      val source = List(
        "List(, und, true)"
      ).toDS

      val actual = getStructuredDataFromText(source)

      val expected = Seq(
        ("", "??", 1)
      ).toDF("text", "language", "label")

      it("should be parsed in correct way") {
        assertSmallDataFrameEquality(actual, expected)
      }
    }
  }
}
