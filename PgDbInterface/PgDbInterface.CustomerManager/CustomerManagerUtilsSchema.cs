using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PgDbInterface.CustomerManager
{
    public class CustomerManagerUtilsSchema : BaseSchemaApi
    {
        public CustomerManagerUtilsSchema(DataBaseConnection connection) : base(connection, "customer_manager_utils")
        {
        }

        /// <summary>
        /// Получить список направлений в рейсе
        /// </summary>
        /// <param name="flightId">ID рейса</param>
        public Task<DataSet> GetFlightStages(int flightId, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_flight_id", flightId }
            };
            return ExecFuncDataSet("get_flight_stages", parameters, cancellationToken);
        }


        /// <summary>
        /// Получить заявки.
        /// <para/>
        /// Данный метод является лишь примером вызова хранимой функции. В нормальных условиях список заявок должен быть получен средствами Entity Framework.
        /// </summary>
        /// <param name="cancellationToken">Токен для отмены вызова функции</param>
        /// <returns></returns>
        public Task<DataSet> GetRequests(CancellationToken cancellationToken = default)
        {
            return ExecFuncDataSet("get_requests", cancellationToken);
        }

        /// <summary>
        /// Получить пассажиров, выполняющих посадку на указанной остановке
        /// </summary>
        /// <param name="destId">ID остановки в рейсе</param>
        public Task<DataSet> GetBoardingPax(int destId, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_dest_id", destId }
            };
            return ExecFuncDataSet("get_boarding_pax", parameters, cancellationToken);
        }

        /// <summary>
        /// Получить пассажиров, следующих транзитом через указанную остановку
        /// </summary>
        /// <param name="destId">ID остановки в рейсе</param>
        public Task<DataSet> GetTransitPax(int destId, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_dest_id", destId }
            };
            return ExecFuncDataSet("get_transit_pax", parameters, cancellationToken);
        }

        /// <summary>
        /// Получить пассажиров, высаживающихся на указанной остановке
        /// </summary>
        /// <param name="destId">ID остановки в рейсе</param>
        public Task<DataSet> GetUnboardingPax(int destId, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_dest_id", destId }
            };
            return ExecFuncDataSet("get_unboarding_pax", parameters, cancellationToken);
        }

        /// <summary>
        /// Получить партии груза, загружаемые на указанной остановке
        /// </summary>
        /// <param name="destId">ID остановки в рейсе</param>
        public Task<DataSet> GetLoadedCargo(int destId, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_dest_id", destId }
            };
            return ExecFuncDataSet("get_loaded_cargo", parameters, cancellationToken);
        }

        /// <summary>
        /// Получить партии груза, следующие транзитом через указанную остановку
        /// </summary>
        /// <param name="destId">ID остановки в рейсе</param>
        public Task<DataSet> GetTransitCargo(int destId, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_dest_id", destId }
            };
            return ExecFuncDataSet("get_transit_cargo", parameters, cancellationToken);
        }

        /// <summary>
        /// Получить партии груза, выгружаемые на указанной остановке
        /// </summary>
        /// <param name="destId">ID остановки в рейсе</param>
        public Task<DataSet> GetUnloadedCargo(int destId, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_dest_id", destId }
            };
            return ExecFuncDataSet("get_unloaded_cargo", parameters, cancellationToken);
        }

        /// <summary>
        /// Получить список направлений, на которые возможна пересадка 
        /// </summary>
        /// <param name="transportIds">Допустимые виды транспорта</param>
        /// <param name="pointId">Пункт пересадки</param>
        /// <param name="date">Желаемая дата пересадки</param>
        /// <param name="airCompanyId">Перевозчик</param>
        public Task<DataSet> GetAvailableTransfers(List<int> transportIds, int pointId, DateTime date,
                                                   int? airCompanyId = null,
                                                   CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_transport_ids", transportIds },
                { "p_point_id", pointId },
                { "p_date", date },
                { "p_air_company_id", airCompanyId },
            };
            return ExecFuncDataSet("get_avail_transfers", parameters, cancellationToken);
        }

        /// <summary>
        /// Получить фактические заявки, по которым выполнена посадка
        /// (для налета)
        /// </summary>
        /// <param name="dateStart">Начало периода</param>
        /// <param name="dateEnd">Конец периода</param>
        public Task<DataSet> GetOrdersCalc(DateTime dateStart, DateTime dateEnd, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_date_start", dateStart },
                { "p_date_end", dateEnd },
            };
            return ExecFuncDataSet("get_orders_calc", parameters, cancellationToken);
        }

        /// <summary>
        /// Закрыть рейс (выполняется пользователем с ролью "Администратор ТН")
        /// </summary>
        /// <param name="flightId">ID рейса</param>
        public Task ApproveFlight(int flightId)
        {
            var parameters = new DbParameters
            {
                { "p_flight_id", flightId }
            };
            return CallProc("approve_flight", parameters);
        }

        public Task<DataSet> CalcFlightTime(DateTime dateBegin, DateTime dateEnd, int[] airCompanies)
        {
            var p = new DbParameters
            {
                { "p_date_begin",  dateBegin},
                { "p_date_end", dateEnd},
                { "p_air_companies", airCompanies}
            };
            return ExecFuncDataSet("fc_calc", p);
        }

        /// <summary>
        /// Получить список уникальных остановок для рейсов по типу транспорта за период
        /// (для налета)
        /// </summary>
        /// <param name="transportId">Тип транспорта</param>
        /// <param name="dateBegin">Начало периода</param>
        /// <param name="dateEnd">Конец периода</param>
        public Task<DataSet> GetUniqueDestinations(int transportId, int airCompanyId, DateTime dateBegin, DateTime dateEnd, CancellationToken cancellationToken = default)
        {
            var parameters = new DbParameters
            {
                { "p_transport_id", transportId },
                { "p_air_company_id", airCompanyId },
                { "p_date_begin", dateBegin },
                { "p_date_end", dateEnd },
            };
            return ExecFuncDataSet("get_unique_destinations", parameters, cancellationToken);
        }
        /// <summary>
        /// Распределить введенное пользователем время направления заявки
        /// </summary>
        /// <param name="orderId">id заявки</param>
        /// <param name="timeUndistr"></param>
        /// <returns></returns>
        public Task SetOrderTime(int orderId, decimal timeUndistr)
        {
            var p = new DbParameters
            {
                { "p_order_id",  orderId},
                { "p_time_undistr", timeUndistr}
            };
            return CallProc("fc_set_order_time", p);
        }

        /// <summary>
        /// Сбосить пользовательский ввод для нефиксированных направлений за период
        /// </summary>
        /// <param name="dateBegin">Начало периода</param>
        /// <param name="dateEnd">Конец периода</param>
        /// <returns></returns>
        public Task CleanUserInput(DateTime dateBegin, DateTime dateEnd)
        {
            var p = new DbParameters
            {
                { "p_date_begin",  dateBegin},
                { "p_date_end", dateEnd}
            };
            return CallProc("fc_clean_user_input", p);
        }
    }
}
