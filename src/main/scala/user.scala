
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}


object user {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("saf").setMaster("local")
    val sc = new SparkContext(conf)
    var conn : Connection = null
    val rdd = new JdbcRDD(
      sc,
      ()=>{
        val driver ="com.mysql.jdbc.Driver"
        val url = "jdbc:mysql://localhost:3306/mydb"
        val userName = "root"
        val password = "123123"
        Class.forName(driver)
        DriverManager.getConnection(url,userName,password)
      },
      "select * from t_user where userId >= ? and userId <=?",
      1,
      5,
      1,
      rs => (rs.getInt("userId"),rs.getString("userName"),rs.getString("passWord"))
    )
    var ps : PreparedStatement = null
    /********** Begin *********/
    val data = sc.parallelize(List((1,"sunyue","123456"), (2,"iteblog", "123481"), (3,"com", "135486")))
    // 请将 RDD 中的用户信息插入 MySQL mydb 数据库的表 t_user 中，列表中的三个元素分别表示 userId、userName和passWord
    try{
      val drive = "com.mysql.jdbc.Driver"
      Class.forName(drive)
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/mydb?characterEncoding=UTF-8","root","123123")
      ps = conn.prepareStatement("insert into user values (?, ?, ?)")
      data.foreach(data=>{
        ps.setInt(1,data._1)
        ps.setString(2,data._2)
        ps.setString(3,data._3)
        ps.executeUpdate()
      })
    }catch{
      case e:SQLException => println(e.printStackTrace())
    }finally{
      if(ps != null) ps.close()
      if(conn != null) conn.close()
    }




    /********** End *********/
    sc.stop()
  }
}