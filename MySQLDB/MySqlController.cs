using System;
using System.Data;
using System.Configuration;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using MySql.Data;
using MySql.Data.MySqlClient;
namespace MySQLDB
{
    public class MySqlController
    {
        //数据库连接字符串
        // public string Conn = "server=localhost;database=test;PORT=3306; uid=root;pwd=testtest;charset=gb2312";

        private MySqlConnection conn;

        /// <summary>
        /// 构造函数
        /// </summary>
        public MySqlController()
        {
            conn = new MySqlConnection();
            conn.ConnectionString = System.Configuration.ConfigurationManager.ConnectionStrings["ConnectionString"].ConnectionString;
        }

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="StrConnection">链接字符串</param>
        public MySqlController(string StrConnection)
        {
            conn = new MySqlConnection();
            conn.ConnectionString = StrConnection;
        }

        /// <summary>
        /// 打开数据库
        /// </summary>
        private void ConnectionOpen()
        {
            try
            {
                if (conn.State != ConnectionState.Open)
                {
                    conn.Open();
                }
            }
            catch (Exception Exp)
            {
                throw (new Exception("打开数据库失败：" + Exp.Message));
            }
        }

        /// <summary>
        /// 关闭数据库 
        /// </summary>
        private void ConnectionClose()
        {
            try
            {
                if (conn.State != ConnectionState.Closed)
                {
                    conn.Close();
                }
            }
            catch (Exception Exp)
            {
                throw (new Exception("关闭数据库失败：" + Exp.Message));
            }
        }

        /// <summary>
        /// 给定连接的数据库用假设参数执行一个sql命令（不返回数据集）
        /// </summary>
        /// <param name="cmdType">命令类型(存储过程, 文本, 等等)</param>
        /// <param name="cmdText">存储过程名称或者sql命令语句</param>
        /// <param name="commandParameters">执行命令所用参数的集合</param>
        /// <returns>执行命令所影响的行数</returns>
        public int ExecuteNonQuery(CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            try
            {
                ConnectionOpen();
                MySqlCommand cmd = new MySqlCommand();

                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                int val = cmd.ExecuteNonQuery();
                cmd.Parameters.Clear();
                return val;
            }
            catch (Exception exc)
            {
                throw exc;
            }
            finally
            {
                ConnectionClose();
            }
        }

        ///// <summary>
        /////使用现有的SQL事务执行一个sql命令（不返回数据集）
        ///// </summary>
        ///// <remarks>
        /////举例:
        ///// int result = ExecuteNonQuery(connString, CommandType.StoredProcedure, "PublishOrders", new MySqlParameter("@prodid", 24));
        ///// </remarks>
        ///// <param name="trans">一个现有的事务</param>
        ///// <param name="cmdType">命令类型(存储过程, 文本, 等等)</param>
        ///// <param name="cmdText">存储过程名称或者sql命令语句</param>
        ///// <param name="commandParameters">执行命令所用参数的集合</param>
        ///// <returns>执行命令所影响的行数</returns>
        //public int ExecuteNonQuery(MySqlTransaction trans, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        //{
        //    MySqlCommand cmd = new MySqlCommand();
        //    PrepareCommand(cmd, trans.Connection, trans, cmdType, cmdText, commandParameters);
        //    int val = cmd.ExecuteNonQuery();
        //    cmd.Parameters.Clear();
        //    return val;
        //}

        ///// <summary>
        ///// 用执行的数据库连接执行一个返回数据集的sql命令
        ///// </summary>
        ///// <remarks>
        ///// 举例:
        ///// MySqlDataReader r = ExecuteReader(connString, CommandType.StoredProcedure, "PublishOrders", new MySqlParameter("@prodid", 24));
        ///// </remarks>
        ///// <param name="connectionString">一个有效的连接字符串</param>
        ///// <param name="cmdType">命令类型(存储过程, 文本, 等等)</param>
        ///// <param name="cmdText">存储过程名称或者sql命令语句</param>
        ///// <param name="commandParameters">执行命令所用参数的集合</param>
        ///// <returns>包含结果的读取器</returns>
        //public  MySqlDataReader ExecuteReader(string connectionString, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        //{
        //    //创建一个MySqlCommand对象
        //    MySqlCommand cmd = new MySqlCommand();
        //    //创建一个MySqlConnection对象
        //    MySqlConnection conn = new MySqlConnection(connectionString);
        //    //在这里我们用一个try/catch结构执行sql文本命令/存储过程，因为如果这个方法产生一个异常我们要关闭连接，因为没有读取器存在，
        //    //因此commandBehaviour.CloseConnection 就不会执行
        //    try
        //    {
        //        //调用 PrepareCommand 方法，对 MySqlCommand 对象设置参数
        //        PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
        //        //调用 MySqlCommand 的 ExecuteReader 方法
        //        MySqlDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
        //        //清除参数
        //        cmd.Parameters.Clear();
        //        return reader;
        //    }
        //    catch
        //    {
        //        //关闭连接，抛出异常
        //        conn.Close();
        //        throw;
        //    }
        //}

        /// <summary>
        /// 返回DataSet
        /// </summary>
        /// <param name="connectionString">一个有效的连接字符串</param>
        /// <param name="cmdType">命令类型(存储过程, 文本, 等等)</param>
        /// <param name="cmdText">存储过程名称或者sql命令语句</param>
        /// <param name="commandParameters">执行命令所用参数的集合</param>
        /// <returns></returns>
        public DataSet GetDataSet(CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            //创建一个MySqlCommand对象
            MySqlCommand cmd = new MySqlCommand();

            //在这里我们用一个try/catch结构执行sql文本命令/存储过程，
            //因为如果这个方法产生一个异常我们要关闭连接，因为没有读取器存在，
            try
            {
                ConnectionOpen();
                //调用 PrepareCommand 方法，对 MySqlCommand 对象设置参数
                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                //调用 MySqlCommand 的 ExecuteReader 方法
                MySqlDataAdapter adapter = new MySqlDataAdapter();
                adapter.SelectCommand = cmd;
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                //清除参数
                cmd.Parameters.Clear();

                return ds;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                ConnectionClose();
            }
        }

        /// <summary>
        /// 返回DataSet
        /// </summary>
        /// <param name="connectionString">一个有效的连接字符串</param>
        /// <param name="cmdType">命令类型(存储过程, 文本, 等等)</param>
        /// <param name="cmdText">存储过程名称或者sql命令语句</param>
        /// <param name="commandParameters">执行命令所用参数的集合</param>
        /// <returns></returns>
        public DataTable GetDataTable(CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            DataSet ds = GetDataSet(cmdType, cmdText, commandParameters);
            if (ds.Tables.Count > 1)
            {
                return ds.Tables[0];
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// 用指定的数据库连接执行一个命令并返回一个数据集的第一列
        /// </summary>
        /// <remarks>
        /// 例如:
        /// Object obj = ExecuteScalar(connString, CommandType.StoredProcedure, "PublishOrders", new MySqlParameter("@prodid", 24));
        /// </remarks>
        /// <param name="connection">一个存在的数据库连接</param>
        /// <param name="cmdType">命令类型(存储过程, 文本, 等等)</param>
        /// <param name="cmdText">存储过程名称或者sql命令语句</param>
        /// <param name="commandParameters">执行命令所用参数的集合</param>
        /// <returns>用 Convert.To{Type}把类型转换为想要的 </returns>
        public object ExecuteScalar(CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        {
            try
            {
                ConnectionOpen();
                MySqlCommand cmd = new MySqlCommand();
                PrepareCommand(cmd, conn, null, cmdType, cmdText, commandParameters);
                object val = cmd.ExecuteScalar();
                cmd.Parameters.Clear();
                return val;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                ConnectionClose();
            }
        }

        /// <summary>
        /// 准备执行一个命令
        /// </summary>
        /// <param name="cmd">sql命令</param>
        /// <param name="conn">OleDb连接</param>
        /// <param name="trans">OleDb事务</param>
        /// <param name="cmdType">命令类型例如 存储过程或者文本</param>
        /// <param name="cmdText">命令文本,例如:Select * from Products</param>
        /// <param name="cmdParms">执行命令的参数</param>
        private void PrepareCommand(MySqlCommand cmd, MySqlConnection conn, MySqlTransaction trans, CommandType cmdType, string cmdText, MySqlParameter[] cmdParms)
        {
            if (conn.State != ConnectionState.Open)
                conn.Open();
            cmd.Connection = conn;
            cmd.CommandText = cmdText;
            if (trans != null)
                cmd.Transaction = trans;
            cmd.CommandType = cmdType;
            if (cmdParms != null)
            {
                foreach (MySqlParameter parm in cmdParms)
                    cmd.Parameters.Add(parm);
            }
        }


        ///// <summary>
        ///// 用指定的数据库连接字符串执行一个命令并返回一个数据集的第一列
        ///// </summary>
        ///// <remarks>
        /////例如:
        ///// Object obj = ExecuteScalar(connString, CommandType.StoredProcedure, "PublishOrders", new MySqlParameter("@prodid", 24));
        ///// </remarks>
        /////<param name="connectionString">一个有效的连接字符串</param>
        ///// <param name="cmdType">命令类型(存储过程, 文本, 等等)</param>
        ///// <param name="cmdText">存储过程名称或者sql命令语句</param>
        ///// <param name="commandParameters">执行命令所用参数的集合</param>
        ///// <returns>用 Convert.To{Type}把类型转换为想要的 </returns>
        //public object ExecuteScalar(string connectionString, CommandType cmdType, string cmdText, params MySqlParameter[] commandParameters)
        //{
        //    MySqlCommand cmd = new MySqlCommand();
        //    using (MySqlConnection connection = new MySqlConnection(connectionString))
        //    {
        //        PrepareCommand(cmd, connection, null, cmdType, cmdText, commandParameters);
        //        object val = cmd.ExecuteScalar();
        //        cmd.Parameters.Clear();
        //        return val;
        //    }
        //}
    }
}