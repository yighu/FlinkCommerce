package FlinkCommerce

import Deserializer.JSONValueDeserializationSchema
import Dto.{SalesPerCategory, SalesPerDay, SalesPerMonth, Transaction}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, RequestIndexer}
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import utils.JsonUtil.convertTransactionToJson
import java.sql.{Date, PreparedStatement}

object DataStreamJobScala extends App {

  private val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
  private val username = "postgres"
  private val password = "postgres"
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val topic = "financial_transactions"

  val source: KafkaSource[Transaction] = KafkaSource.builder[Transaction].setBootstrapServers("localhost:9092").setTopics(topic).setGroupId("flink-group").setStartingOffsets(OffsetsInitializer.earliest).setValueOnlyDeserializer(new JSONValueDeserializationSchema).build

  val transactionStream : DataStream[Transaction] = env.fromSource(source, WatermarkStrategy.noWatermarks[Transaction], "Kafka source")

  //transactionStream.print
  val execOptions = new JdbcExecutionOptions.Builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build

  val connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(jdbcUrl).withDriverName("org.postgresql.Driver").withUsername(username).withPassword(password).build

  //create transactions table
  transactionStream.addSink(JdbcSink.sink("CREATE TABLE IF NOT EXISTS transactions (" + "transaction_id VARCHAR(255) PRIMARY KEY, " + "product_id VARCHAR(255), " + "product_name VARCHAR(255), " + "product_category VARCHAR(255), " + "product_price DOUBLE PRECISION, " + "product_quantity INTEGER, " + "product_brand VARCHAR(255), " + "total_amount DOUBLE PRECISION, " + "currency VARCHAR(255), " + "customer_id VARCHAR(255), " + "transaction_date TIMESTAMP, " + "payment_method VARCHAR(255) " + ")",
    new JdbcStatementBuilder[Transaction] {
      override def accept(t: PreparedStatement, u: Transaction): Unit = {}
    },
    execOptions,
    connOptions
  )
  ).name("Create Transactions Table Sink")

  //create sales_per_category table sink
  transactionStream.addSink(JdbcSink.sink("CREATE TABLE IF NOT EXISTS sales_per_category (" + "transaction_date DATE, " + "category VARCHAR(255), " + "total_sales DOUBLE PRECISION, " + "PRIMARY KEY (transaction_date, category)" + ")",
   new JdbcStatementBuilder[Transaction] {
     override def accept(t: PreparedStatement, u: Transaction): Unit = {}
   },
    execOptions, connOptions)).name("Create Sales Per Category Table")

  //create sales_per_day table sink
  transactionStream.addSink(JdbcSink.sink("CREATE TABLE IF NOT EXISTS sales_per_day (" + "transaction_date DATE PRIMARY KEY, " + "total_sales DOUBLE PRECISION " + ")",
    new JdbcStatementBuilder[Transaction] {
      override def accept(t: PreparedStatement, u: Transaction): Unit = {}
    }, execOptions, connOptions)).name("Create Sales Per Day Table")

  //create sales_per_month table sink
  transactionStream.addSink(JdbcSink.sink("CREATE TABLE IF NOT EXISTS sales_per_month (" + "year INTEGER, " + "month INTEGER, " + "total_sales DOUBLE PRECISION, " + "PRIMARY KEY (year, month)" + ")",
    new JdbcStatementBuilder[Transaction] {
      override def accept(t: PreparedStatement, u: Transaction): Unit = {}
    }, execOptions, connOptions)).name("Create Sales Per Month Table")

  transactionStream.addSink(JdbcSink.sink("INSERT INTO transactions(transaction_id, product_id, product_name, product_category, product_price, " + "product_quantity, product_brand, total_amount, currency, customer_id, transaction_date, payment_method) " + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " + "ON CONFLICT (transaction_id) DO UPDATE SET " + "product_id = EXCLUDED.product_id, " + "product_name  = EXCLUDED.product_name, " + "product_category  = EXCLUDED.product_category, " + "product_price = EXCLUDED.product_price, " + "product_quantity = EXCLUDED.product_quantity, " + "product_brand = EXCLUDED.product_brand, " + "total_amount  = EXCLUDED.total_amount, " + "currency = EXCLUDED.currency, " + "customer_id  = EXCLUDED.customer_id, " + "transaction_date = EXCLUDED.transaction_date, " + "payment_method = EXCLUDED.payment_method " + "WHERE transactions.transaction_id = EXCLUDED.transaction_id",
    new JdbcStatementBuilder[Transaction] {
      override def accept(preparedStatement: PreparedStatement, transaction: Transaction): Unit = {
        preparedStatement.setString(1, transaction.getTransactionId)
        preparedStatement.setString(2, transaction.getProductId)
        preparedStatement.setString(3, transaction.getProductName)
        preparedStatement.setString(4, transaction.getProductCategory)
        preparedStatement.setDouble(5, transaction.getProductPrice)
        preparedStatement.setInt(6, transaction.getProductQuantity)
        preparedStatement.setString(7, transaction.getProductBrand)
        preparedStatement.setDouble(8, transaction.getTotalAmount)
        preparedStatement.setString(9, transaction.getCurrency)
        preparedStatement.setString(10, transaction.getCustomerId)
        preparedStatement.setTimestamp(11, transaction.getTransactionDate)
        preparedStatement.setString(12, transaction.getPaymentMethod)
      }
  }, execOptions, connOptions)).name("Insert into transactions table sink")
   transactionStream.map(transaction => new SalesPerCategory(new Date(System.currentTimeMillis), transaction.getProductCategory, transaction.getTotalAmount))
     .keyBy(new KeySelector[SalesPerCategory, String]{
       override def getKey(in: SalesPerCategory): String = in.getCategory
     }).reduce((s, t) => {
      s.setTotalSales(s.getTotalSales  + t.getTotalSales)
      s
  }).addSink(JdbcSink.sink("INSERT INTO sales_per_category(transaction_date, category, total_sales) " + "VALUES (?, ?, ?) " + "ON CONFLICT (transaction_date, category) DO UPDATE SET " + "total_sales = EXCLUDED.total_sales " + "WHERE sales_per_category.category = EXCLUDED.category " + "AND sales_per_category.transaction_date = EXCLUDED.transaction_date",
     new JdbcStatementBuilder[SalesPerCategory] {
       override def accept(preparedStatement: PreparedStatement, salesPerCategory: SalesPerCategory): Unit = {
         preparedStatement.setDate(1, new Date(System.currentTimeMillis))
         preparedStatement.setString(2, salesPerCategory.getCategory)
         preparedStatement.setDouble(3, salesPerCategory.getTotalSales)
       }
  }, execOptions, connOptions)).name("Insert into sales per category table")

  transactionStream.map((transaction: Transaction) => {

      val transactionDate = new Date(System.currentTimeMillis)
      val totalSales = transaction.getTotalAmount
      new SalesPerDay(transactionDate, totalSales)

  }).keyBy(new KeySelector[SalesPerDay, Date] {
    override def getKey(in: SalesPerDay): Date = in.getTransactionDate
  }).reduce((s: SalesPerDay, t: SalesPerDay) => {
      s.setTotalSales(s.getTotalSales + t.getTotalSales)
      s
  }).addSink(JdbcSink.sink("INSERT INTO sales_per_day(transaction_date, total_sales) " + "VALUES (?,?) " + "ON CONFLICT (transaction_date) DO UPDATE SET " + "total_sales = EXCLUDED.total_sales " + "WHERE sales_per_day.transaction_date = EXCLUDED.transaction_date",
    new JdbcStatementBuilder[SalesPerDay] {
      override def accept(preparedStatement: PreparedStatement, salesPerDay: SalesPerDay): Unit =   {
      preparedStatement.setDate(1, new Date(System.currentTimeMillis))
      preparedStatement.setDouble(2, salesPerDay.getTotalSales)
     }
    }, execOptions, connOptions)).name("Insert into sales per day table")

  transactionStream.map((transaction: Transaction) => {
      val transactionDate = new Date(System.currentTimeMillis)
      val year = transactionDate.toLocalDate.getYear
      val month = transactionDate.toLocalDate.getMonth.getValue
      val totalSales = transaction.getTotalAmount
      new SalesPerMonth(year, month, totalSales)

  }).keyBy(new KeySelector[SalesPerMonth, Int] {
    override def getKey(in: SalesPerMonth): Int = in.getMonth
  }).reduce((s: SalesPerMonth, t: SalesPerMonth) => {
      s.setTotalSales(s.getTotalSales + t.getTotalSales)
      s
   
  }).addSink(JdbcSink.sink("INSERT INTO sales_per_month(year, month, total_sales) " + "VALUES (?,?,?) " + "ON CONFLICT (year, month) DO UPDATE SET " + "total_sales = EXCLUDED.total_sales " + "WHERE sales_per_month.year = EXCLUDED.year " + "AND sales_per_month.month = EXCLUDED.month ",
    new JdbcStatementBuilder[SalesPerMonth] {
      override def accept(preparedStatement: PreparedStatement, salesPerMonth: SalesPerMonth): Unit = {
        preparedStatement.setInt(1, salesPerMonth.getYear)
        preparedStatement.setInt(2, salesPerMonth.getMonth)
        preparedStatement.setDouble(3, salesPerMonth.getTotalSales)
      }
  }, execOptions, connOptions)).name("Insert into sales per month table")

  val elasticsearchSink:Elasticsearch7SinkBuilder[Transaction] = new Elasticsearch7SinkBuilder[Transaction]()
    .setHosts(new HttpHost("localhost", 9200, "http"))
    .setEmitter((transaction: Transaction, runtimeContext: SinkWriter.Context, requestIndexer: RequestIndexer) => {
      val json = convertTransactionToJson(transaction)
      val indexRequest = Requests.indexRequest.index("transactions")
        .id(transaction.getTransactionId)
        .source(json, XContentType.JSON)
      requestIndexer.add(indexRequest)
    })
  transactionStream.sinkTo(elasticsearchSink.build()).name("Elasticsearch Sink")

  // Execute program, beginning computation.
  env.execute("Flink Ecommerce Realtime Streaming Scala")
}
