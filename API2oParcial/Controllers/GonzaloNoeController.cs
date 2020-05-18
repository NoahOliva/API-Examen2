using Microsoft.AnalysisServices.AdomdClient;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using System.Web.Http.Cors;

namespace API2oParcial.Controllers
{
    [EnableCors(origins: "*", headers: "*", methods: "*")]
    [RoutePrefix("p2/GonzaloNoe/Northwind")]
    public class GonzaloNoeController : ApiController
    {
        [HttpGet]
        [Route("Testing")]
        public HttpResponseMessage Testing()
        {
            return Request.CreateResponse(HttpStatusCode.OK, "Prueba de API Exitosa");
        }
        [HttpGet]
        [Route("HistoricoAnio/{anio}/{mes}")]
        public HttpResponseMessage Historico(string anio, int mes)
        {
            string dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
            string order = "DESC";


            string WITH = @"
                WITH 
                SET [TopVentas] AS 
                NONEMPTY(
                    ORDER(
                        STRTOSET(@Dimension),
                        [Measures].[Hechos Ventas Netas], " + order +
                    @")
                )
            ";

            string COLUMNS = @"
                NON EMPTY
                {
                    [Dim Tiempo].[Anio].&["+ anio + @"]
                }
            *
				{
                [Dim Tiempo].[Numero Mes].&[" + mes + @"]
                }
                ON COLUMNS,    
            ";
            string ROWS = @"
                NON EMPTY
                {
                    [TopVentas]
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> clients = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosTabla = lstTabla
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", dimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            clients.Add(dr.GetString(0));

                            decimal anioRes;
                            if (dr.IsDBNull(1))
                                anioRes = 0;
                            else
                                anioRes = Math.Round(dr.GetDecimal(1));
                            

                            dynamic objTabla = new
                            {
                                cliente = dr.GetString(0),
                                anioRes
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }


        [HttpGet]
        [Route("Historico")]
        public HttpResponseMessage Historico()
        {
            string dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
            string order = "DESC";


            string WITH = @"
                WITH 
                SET [TopVentas] AS 
                NONEMPTY(
                    ORDER(
                        STRTOSET(@Dimension),
                        [Measures].[Hechos Ventas Netas], " + order +
                    @")
                )
            ";

            string COLUMNS = @"
                NON EMPTY
                {
                    [Dim Tiempo].[Anio].children
                }
                ON COLUMNS,    
            ";

            string ROWS = @"
                NON EMPTY
                {
                    [TopVentas]
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> clients = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosTabla = lstTabla
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", dimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            clients.Add(dr.GetString(0));

                            decimal anio96, anio97, anio98;
                            if (dr.IsDBNull(1))
                                anio96 = 0;
                            else
                                anio96 = Math.Round(dr.GetDecimal(1));

                            if (dr.IsDBNull(2))
                                anio97 = 0;
                            else
                                anio97 = Math.Round(dr.GetDecimal(2));

                            if (dr.IsDBNull(3))
                                anio98 = 0;
                            else
                                anio98 = Math.Round(dr.GetDecimal(3));


                            dynamic objTabla = new
                            {
                                cliente = dr.GetString(0),
                                anio96,
                                anio97,
                                anio98
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }


        [HttpGet]
        [Route("Top5/{dim}/{order}")]
        public HttpResponseMessage Top5(string dim, string order = "DESC")
        {
            string dimension = string.Empty;

            switch (dim)
            {
                case "Cliente":
                    dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
                    break;
                case "Producto":
                    dimension = "[Dim Producto].[Dim Producto Nombre].CHILDREN";
                    break;
                case "Empleado":
                    dimension = "[Dim Empleado].[Dim Empleado Nombre].CHILDREN";
                    break;
                default:
                    dimension = "[Dim Cliente].[Dim Cliente Nombre].CHILDREN";
                    break;
            }

            string WITH = @"
                WITH 
                SET [TopVentas] AS 
                NONEMPTY(
                    ORDER(
                        STRTOSET(@Dimension),
                        [Measures].[Hechos Ventas Netas], " + order +
                    @")
                )
            ";

            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hechos Ventas Netas]
                }
                ON COLUMNS,    
            ";

            string ROWS = @"
                NON EMPTY
                {
                    HEAD([TopVentas], 5)
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> clients = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosDimension = clients,
                datosVenta = ventas,
                datosTabla = lstTabla
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", dimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            clients.Add(dr.GetString(0));
                            ventas.Add(Math.Round(dr.GetDecimal(1)));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                valor = Math.Round(dr.GetDecimal(1))
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }


        [HttpGet]
        [Route("GetItemsByDimension/{dim}/{order}/{anio}/{mes}")]
        public HttpResponseMessage GetItemsByDimension(string dim, string anio, int mes, string order = "DESC")
        {
            string WITH = @"
                WITH 
                SET [OrderDimension] AS 
                NONEMPTY(
                    ORDER(
                        {0}.CHILDREN,
                        {0}.CURRENTMEMBER.MEMBER_NAME, " + order +
                    @")
                )
            ";

            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hechos Ventas Netas]
                }
            *
                {
                    [Dim Tiempo].[Anio].&[" + anio + @"]
                }
            *
				{
                [Dim Tiempo].[Numero Mes].&[" + mes + @"]
                }
                ON COLUMNS,    
            ";

            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            WITH = string.Format(WITH, dim);
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> dimension = new List<string>();

            dynamic result = new
            {
                datosDimension = dimension
            };

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    //cmd.Parameters.Add("Dimension", dimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }

        [HttpPost]
        [Route("GetDataPieByDimension/{dim}/{order}/{anio}/{mes}")]
        public HttpResponseMessage GetDataPieByDimension(string dim, string order, string anio, int mes, string[] values)
        {
            string WITH = @"
            WITH 
                SET [OrderDimension] AS 
                NONEMPTY(
                    ORDER(
			        STRTOSET(@Dimension),
                    [Measures].[Hechos Ventas Netas], DESC
	            )
            )
            ";

            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hechos Ventas Netas]
                }
                *
                {
                    [Dim Tiempo].[Anio].&[" + anio + @"]
                }
            *
				{
                [Dim Tiempo].[Numero Mes].&[" + mes + @"]
                }
                ON COLUMNS,    
            ";

            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            //WITH = string.Format(WITH, dim);
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);

            List<string> dimension = new List<string>();
            List<decimal> ventas = new List<decimal>();
            List<dynamic> lstTabla = new List<dynamic>();

            dynamic result = new
            {
                datosDimension = dimension,
                datosVenta = ventas,
                datosTabla = lstTabla
            };

            string valoresDimension = string.Empty;
            foreach (var item in values)
            {
                valoresDimension += "{0}.[" + item + "],";
            }
            valoresDimension = valoresDimension.TrimEnd(',');
            valoresDimension = string.Format(valoresDimension, dim);
            valoresDimension = @"{" + valoresDimension + "}";

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", valoresDimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            dimension.Add(dr.GetString(0));
                            ventas.Add(Math.Round(dr.GetDecimal(1)));

                            dynamic objTabla = new
                            {
                                descripcion = dr.GetString(0),
                                valor = Math.Round(dr.GetDecimal(1))
                            };

                            lstTabla.Add(objTabla);
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }


        [HttpPost]
        [Route("GetDataChartByDimension/{dim}/{order}/{anio}/{meses}")]
        public HttpResponseMessage GetDataChartByDimension(string dim, string order, string anio, string meses, string[] values)
        {
            string WITH = @"
            WITH 
                SET [OrderDimension] AS 
                NONEMPTY(
                    ORDER(
			        STRTOSET(@Dimension),
                    [Measures].[Hechos Ventas Netas], DESC
	            )
            )
            ";
            string MESES = "";
            var mesesLista = meses.Split(',');

            for (int i = 0; i < mesesLista.Length -1; i++)
            {
                MESES += "[Dim Tiempo].[Numero Mes].&[" + mesesLista[i] + @"]";
                if (i != mesesLista.Length - 2) MESES += ',';
            }

            string COLUMNS = @"
                NON EMPTY
                {
                    [Measures].[Hechos Ventas Netas]
                }
                *
                {
                    [Dim Tiempo].[Anio].&[" + anio + @"]
                }
            *
				{
                " + MESES + @"
                }
                ON COLUMNS,    
            ";

            string ROWS = @"
                NON EMPTY
                {
                    [OrderDimension]
                }
                ON ROWS
            ";

            string CUBO_NAME = "[DWH Northwind]";
            //WITH = string.Format(WITH, dim);
            string MDX_QUERY = WITH + @"SELECT " + COLUMNS + ROWS + " FROM " + CUBO_NAME;

            Debug.Write(MDX_QUERY);
            
            List<List<decimal>> ventas = new List<List<decimal>>();

            dynamic result = new
            {
                datosVenta = ventas
            };

            string valoresDimension = string.Empty;
            foreach (var item in values)
            {
                valoresDimension += "{0}.[" + item + "],";
            }
            valoresDimension = valoresDimension.TrimEnd(',');
            valoresDimension = string.Format(valoresDimension, dim);
            valoresDimension = @"{" + valoresDimension + "}";

            using (AdomdConnection cnn = new AdomdConnection(ConfigurationManager.ConnectionStrings["CuboNorthwind"].ConnectionString))
            {
                cnn.Open();
                using (AdomdCommand cmd = new AdomdCommand(MDX_QUERY, cnn))
                {
                    cmd.Parameters.Add("Dimension", valoresDimension);
                    using (AdomdDataReader dr = cmd.ExecuteReader(CommandBehavior.CloseConnection))
                    {
                        while (dr.Read())
                        {
                            List<decimal> unCliente = new List<decimal>();
                            for (int i = 0; i < mesesLista.Length - 1; i++)
                            {
                                decimal anioRes;
                                if (dr.IsDBNull(i+1))
                                    anioRes = 0;
                                else
                                    anioRes = Math.Round(dr.GetDecimal(i+1));
                                unCliente.Add(anioRes);
                            }
                            ventas.Add(unCliente);
                            
                        }
                        dr.Close();
                    }
                }
            }

            return Request.CreateResponse(HttpStatusCode.OK, (object)result);
        }
    }
}
