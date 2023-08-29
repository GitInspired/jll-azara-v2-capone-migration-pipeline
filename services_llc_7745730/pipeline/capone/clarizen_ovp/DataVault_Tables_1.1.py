# Databricks notebook source
# MAGIC %md
# MAGIC **Notebook is used to create below tables**
# MAGIC 1. stage_clarizen_dbo_workitem
# MAGIC 2.  datavault_dbo_r_client_mapping
# MAGIC 3.  datavault_dbo_s_users
# MAGIC 4.  datavault_dbo_s_risk_details
# MAGIC 5.  datavault_dbo_s_project_details
# MAGIC 6.  datavault_dbo_s_related_work_link
# MAGIC 7.  datavault_dbo_r_clients
# MAGIC 8.  datavault_dbo_s_program_details
# MAGIC 9. datavault_dbo_r_client_mapping
# MAGIC 19. datavault_dbo_r_c_directory_contacts
# MAGIC 11. datavault_dbo_s_project_address
# MAGIC 12. datavault_dbo_r_clz_spend_role
# MAGIC 13. datavault_dbo_s_customer
# MAGIC 14. datavault_dbo_s_c_job_site
# MAGIC 15. datavault_dbo_s_project_milestones
# MAGIC 16. datavault_dbo_s_clz_directory_contact_project_links
# MAGIC 17. datavault_dbo_s_project_milestones
# MAGIC 18. datavault_dbo_s_work_item
# MAGIC 19. datavault_dbo_s_work_item_details
# MAGIC 20. datavault_dbo_s_project_customer
# MAGIC 21. datavault_dbo_s_Project_financials
# MAGIC 22. datavault_dbo_s_Project_financial_Details
# MAGIC 23. datavault_dbo_s_clz_project_status_indicator
# MAGIC 24. datavault_dbo_r_clz_service_type
# MAGIC 25. datavault_dbo_r_clz_JLL_Project_Types
# MAGIC 26. datavault_dbo_r_clz_custom_value
# MAGIC 27. datavault_dbo_s_work_item_task_details

# COMMAND ----------

# DBTITLE 1,client_variables
import os
from azarautils import ClientObject

# Client Config
client_obj           = ClientObject(client_id=os.getenv("CLIENT_ID"),client_name=os.getenv("CLIENT_NAME"))
client_secret_scope  = client_obj.client_secret_scope
catalog              = client_obj.catalog

var_client_id        = os.getenv("CLIENT_ID")
var_client_name      = os.getenv("CLIENT_NAME")

var_azara_raw_db     = f"{catalog}.jll_azara_raw"

var_client_raw_db    = f"{catalog}.{client_obj.databricks_client_raw_db}"
var_client_custom_db = f"{catalog}.{client_obj.databricks_client_custom_db}"

# COMMAND ----------

# DBTITLE 1,Assigning "refresh_date" (Updatedate) value
from datetime import timezone
import datetime

# Creating "Data Refresh Date"
_date = datetime.datetime.now(timezone.utc)
jobRunDate = dbutils.jobs.taskValues.get(taskKey="init_pipeline",key="UpdateDate",debugValue='{_date}'.format(_date=_date))
refresh_date = datetime.datetime.strptime(jobRunDate, '%Y-%m-%d %H:%M:%S.%f%z')

# COMMAND ----------

# DBTITLE 1,stage_clarizen_dbo_workitem
spark.sql(""" 
CREATE OR REPLACE TABLE 
    {var_client_custom_db}.stage_clarizen_dbo_workitem as

with _workitem as (
 SELECT
	cast(batch_id as char(32))                                   as batch_id
   ,cast(batch_timestamp as timestamp)                           as batch_timestamp
   ,cast(is_deleted as boolean)                                  as is_deleted
   ,cast(id as varchar(100))                                     as id
   ,cast(CreatedBy as varchar(200))                              as CreatedBy
   ,cast(CreatedOn as timestamp)                                 as CreatedOn
   ,cast(LastUpdatedBy as varchar(200))                          as LastUpdatedBy
   ,cast(LastUpdatedOn as timestamp)                             as LastUpdatedOn
   ,cast(Name as varchar(500))                                   as Name
   ,cast(Description as varchar(2000))                           as Description
   ,cast(ExternalID as varchar(1950))                            as ExternalID
   ,cast(State as varchar(2000))                                 as State
   ,cast(StartDate as timestamp)                                 as StartDate
   ,cast(DueDate as timestamp)                                   as DueDate
   ,cast(Duration as decimal(19,4))                              as Duration
   ,cast(ActualStartDate as timestamp)                           as ActualStartDate
   ,cast(ActualEndDate as timestamp)                             as ActualEndDate
   ,cast(TrackStatus as varchar(2000))                           as TrackStatus
   ,cast(Conflicts as decimal(19,4))                             as Conflicts
   ,cast(OnCriticalPath as boolean)                              as OnCriticalPath
   ,cast(DurationManuallySet as boolean)                         as DurationManuallySet
   ,cast(TrackStatusManuallySet as boolean)                      as TrackStatusManuallySet
   ,cast(EarliestStartDate as timestamp)                         as EarliestStartDate
   ,cast(LatestStartDate as timestamp)                           as LatestStartDate
   ,cast(EarliestEndDate as timestamp)                           as EarliestEndDate
   ,cast(LatestEndDate as timestamp)                             as LatestEndDate
   ,cast(ExpectedProgress as decimal(38,10))                     as ExpectedProgress
   ,cast(SchedulingType as varchar(2000))                        as SchedulingType
   ,cast(ImportedFrom as varchar(2000))                          as ImportedFrom
   ,cast(Importance as varchar(2000))                            as Importance
   ,cast(Priority as int)                                        as Priority
   ,cast(PercentCompleted as decimal(38,10))                     as PercentCompleted
   ,cast(Manager as varchar(200))                                as Manager
   ,cast(ChargedTypeManuallySet as boolean)                      as ChargedTypeManuallySet
   ,cast(ChildrenCount as int)                                   as ChildrenCount
   ,cast(SuccessorsCount as int)                                 as SuccessorsCount
   ,cast(PredecessorsCount as int)                               as PredecessorsCount
   ,cast(AlluserResourcesCount as int)                           as AlluserResourcesCount
   ,cast(AttachmentsCount as int)                                as AttachmentsCount
   ,cast(PostsCount as int)                                      as PostsCount
   ,cast(NotesCount as int)                                      as NotesCount
   ,cast(Reportable as boolean)                                  as Reportable
   ,cast(ReportableManuallySet as boolean)                       as ReportableManuallySet
   ,cast(Billable as boolean)                                    as Billable
   ,cast(ChildShortcutCount as int)                              as ChildShortcutCount
   ,cast(Project as varchar(200))                                as Project
   ,cast(WorkPolicy as varchar(2000))                            as WorkPolicy
   ,cast(CommitLevel as varchar(2000))                           as CommitLevel
   ,cast(ReportingStartDate as timestamp)                        as ReportingStartDate
   ,cast(SYSID as varchar(40))                                   as SYSID
   ,cast(Work as decimal(19,4))                                  as Work
   ,cast(ActualEffort as decimal(19,4))                          as ActualEffort
   ,cast(RemainingEffort as decimal(19,4))                       as RemainingEffort
   ,cast(WorkManuallySet as boolean)                             as WorkManuallySet
   ,cast(RemainingEffortManuallySet as boolean)                  as RemainingEffortManuallySet
--    ,cast(WorkVariance as decimal(19,4))                          as WorkVariance
    ,null                                                       as WorkVariance
   ,cast(ActualDuration as decimal(19,4))                        as ActualDuration
   ,cast(StartDateVariance as decimal(19,4))                     as StartDateVariance
   ,cast(ActualCost as decimal(19,4))                            as ActualCost
   ,cast(DueDateVariance as decimal(19,4))                       as DueDateVariance
   ,cast(PlannedBudget as decimal(19,4))                         as PlannedBudget
   ,cast(DurationVariance as decimal(19,4))                      as DurationVariance
   ,cast(ActualCostManuallySet as boolean)                       as ActualCostManuallySet
   ,cast(PlannedBudgetManuallySet as boolean)                    as PlannedBudgetManuallySet
   ,cast(TimeTrackingEffort as decimal(19,4))                    as TimeTrackingEffort
   ,cast(TimeTrackingCost as decimal(19,4))                      as TimeTrackingCost
   ,cast(FixedCost as decimal(19,4))                             as FixedCost
   ,cast(FixedPrice as decimal(19,4))                            as FixedPrice
   ,cast(PercentInvested as decimal(38,10))                      as PercentInvested
   ,cast(CostVariance as decimal(19,4))                          as CostVariance
   ,cast(TimeTrackingBilling as decimal(19,4))                   as TimeTrackingBilling
   ,cast(EarnedValue as decimal(19,4))                           as EarnedValue
   ,cast(PlannedRevenue as decimal(19,4))                        as PlannedRevenue
   ,cast(CPI as decimal(38,10))                                  as CPI
   ,cast(ActualRevenue as decimal(19,4))                         as ActualRevenue
   ,cast(SPI as decimal(38,10))                                  as SPI
   ,cast(PlannedRevenueManuallySet as boolean)                   as PlannedRevenueManuallySet
   ,cast(ActualRevenueManuallySet as boolean)                    as ActualRevenueManuallySet
   ,cast(Profitability as decimal(19,4))                         as Profitability
   ,cast(PercentProfitability as decimal(38,10))                 as PercentProfitability
   ,cast(PlannedExpenses as decimal(19,4))                       as PlannedExpenses
   ,cast(DirectPlannedExpenses as decimal(19,4))                 as DirectPlannedExpenses
   ,cast(ActualExpenses as decimal(19,4))                        as ActualExpenses
   ,cast(DirectActualExpenses as decimal(19,4))                  as DirectActualExpenses
   ,cast(ProjectedExpenses as decimal(19,4))                     as ProjectedExpenses
   ,cast(DirectProjectedExpenses as decimal(19,4))               as DirectProjectedExpenses
   ,cast(PlannedBilledExpenses as decimal(19,4))                 as PlannedBilledExpenses
   ,cast(DirectPlannedBilledExpenses as decimal(19,4))           as DirectPlannedBilledExpenses
   ,cast(ActualBilledExpenses as decimal(19,4))                  as ActualBilledExpenses
   ,cast(DirectActualBilledExpenses as decimal(19,4))            as DirectActualBilledExpenses
   ,cast(ProjectedBilledExpenses as decimal(19,4))               as ProjectedBilledExpenses
   ,cast(DirectProjectedBilledExpenses as decimal(19,4))         as DirectProjectedBilledExpenses
   ,cast(RevenueCurrencyType as varchar(2000))                   as RevenueCurrencyType
   ,cast(CostCurrencyType as varchar(2000))                      as CostCurrencyType
   ,cast(Pending as varchar(2000))                               as Pending
   ,cast(IssuesCount as int)                                     as IssuesCount
   ,cast(LastUpdatedBySystemOn as timestamp)                     as LastUpdatedBySystemOn
   ,cast(AllowReportingOnSubItems as boolean)                    as AllowReportingOnSubItems
   ,cast(CommittedDate as timestamp)                             as CommittedDate
   ,cast(userResourcesCount as int)                              as userResourcesCount
   ,cast(BudgetedHours as decimal(19,4))                         as BudgetedHours
   ,cast(EmailsCount as int)                                     as EmailsCount
   ,cast(CostBalance as decimal(19,4))                           as CostBalance
   ,cast(BudgetStatus as varchar(2000))                          as BudgetStatus
   ,cast(RevenueBalance as decimal(19,4))                        as RevenueBalance
   ,cast(ActualEffortUpdatedFromTimesheets as boolean)           as ActualEffortUpdatedFromTimesheets
   ,cast(ParentProject as varchar(200))                          as ParentProject
   ,cast(SfExternalId as varchar(256))                           as SfExternalId
   ,cast(SfExternalName as varchar(256))                         as SfExternalName
   ,cast(InternalId as varchar(512))                             as InternalId
   ,cast(OrderID as varchar(512))                                as OrderID
   ,cast(SKU as varchar(512))                                    as SKU
   ,cast(BaselineStartDate as timestamp)                         as BaselineStartDate
   ,cast(BaselineStartDateVariance as decimal(19,4))             as BaselineStartDateVariance
   ,cast(BaselineDueDate as timestamp)                           as BaselineDueDate
   ,cast(BaselineDueDateVariance as decimal(19,4))               as BaselineDueDateVariance
   ,cast(BaselineDuration as decimal(19,4))                      as BaselineDuration
   ,cast(BaselineDurationVariance as decimal(19,4))              as BaselineDurationVariance
   ,cast(BaselineWork as decimal(19,4))                          as BaselineWork
   ,cast(BaselineWorkVariance as decimal(19,4))                  as BaselineWorkVariance
   ,cast(BaselineCost as decimal(19,4))                          as BaselineCost
   ,cast(BaselineCostsVariance as decimal(19,4))                 as BaselineCostsVariance
   ,cast(BaselineRevenue as decimal(19,4))                       as BaselineRevenue
   ,cast(BaselineRevenueVariance as decimal(19,4))               as BaselineRevenueVariance
   ,cast(InternalStatus as varchar(512))                         as InternalStatus
   ,cast(Deliverable as boolean)                                 as Deliverable
   ,cast(DeliverableType as varchar(512))                        as DeliverableType
   ,cast(Executable as boolean)                                  as Executable
   ,cast(Parent as varchar(200))                                 as Parent
   ,cast(PlannedAmount as decimal(19,4))                         as PlannedAmount
   ,cast(ChargedAmount as decimal(19,4))                         as ChargedAmount
   ,cast(TCPI as decimal(38,10))                                 as TCPI
   ,cast(TotalEstimatedCost as decimal(19,4))                    as TotalEstimatedCost
   ,cast(Charged as varchar(2000))                               as Charged
   ,cast(ChargedAmountManuallySet as boolean)                    as ChargedAmountManuallySet
   ,cast(RevenueEarnedValue as decimal(19,4))                    as RevenueEarnedValue
   ,cast(RPI as decimal(38,10))                                  as RPI
   ,cast(RTCPI as decimal(38,10))                                as RTCPI
   ,cast(ReportableStartDate as timestamp)                       as ReportableStartDate
   ,cast(EntityType as varchar(200))                             as EntityType
   ,cast(CompletnessDefinition as decimal(19,4))                 as CompletnessDefinition
   ,cast(ReportableEndDate as timestamp)                         as ReportableEndDate
   ,cast(TaskReportingPolicy as varchar(2000))                   as TaskReportingPolicy
   ,cast(TaskReportingPolicyManuallySet as boolean)              as TaskReportingPolicyManuallySet
   ,cast(FloatingTask as boolean)                                as FloatingTask
   ,cast(IndividualReporting as boolean)                         as IndividualReporting
   ,cast(BaselineCreationDate as timestamp)                      as BaselineCreationDate
   ,cast(AggregatedStopwatchesCount as int)                      as AggregatedStopwatchesCount
   ,cast(CalculateCompletenessBasedOnEfforts as boolean)         as CalculateCompletenessBasedOnEfforts
   ,cast(ActiveStopwatch as varchar(2000))                       as ActiveStopwatch
   ,cast(ObjectAlias as varchar(1000))                           as ObjectAlias
   ,cast(StopwatchesCount as int)                                as StopwatchesCount
   ,cast(LikesCount as int)                                      as LikesCount
   ,cast(BudgetedHoursManuallySet as boolean)                    as BudgetedHoursManuallySet
   ,cast(CurrencyEAC as decimal(28,4))                           as CurrencyEAC
   ,cast(PendingTimeTrackingEffort as decimal(19,4))             as PendingTimeTrackingEffort
   ,cast(CurrencyREAC as decimal(19,4))                          as CurrencyREAC
   ,cast(CurrencyETC as decimal(28,4))                           as CurrencyETC
   ,cast(ImageUrl as varchar(256))                               as ImageUrl
   ,cast(CurrencyRETC as decimal(19,4))                          as CurrencyRETC
   ,cast(SetByLeveling as decimal(19,4))                         as SetByLeveling
   ,cast(GeographicalRegion as varchar(2000))                    as GeographicalRegion
   ,cast(ResourceUtilizationCategory as varchar(2000))           as ResourceUtilizationCategory
   ,cast(StateProvince as varchar(2000))                         as StateProvince
   ,cast(ExpenseType as varchar(2000))                           as ExpenseType
   ,cast(BudgetCostOPEX as decimal(19,4))                        as BudgetCostOPEX
   ,cast(BudgetCostCAPEX as decimal(19,4))                       as BudgetCostCAPEX
   ,cast(BudgetCostNLR as decimal(19,4))                         as BudgetCostNLR
   ,cast(BudgetCostLR as decimal(19,4))                          as BudgetCostLR
   ,cast(EntityOwner as varchar(200))                            as EntityOwner
   ,cast(ActualCostOPEX as decimal(19,4))                        as ActualCostOPEX
   ,cast(ActualCostCAPEX as decimal(19,4))                       as ActualCostCAPEX
   ,cast(ActualCostNLR as decimal(19,4))                         as ActualCostNLR
   ,cast(ActualCostNLROpex as decimal(19,4))                     as ActualCostNLROpex
   ,cast(ActualCostNLRCapex as decimal(19,4))                    as ActualCostNLRCapex
   ,cast(ActualCostLR as decimal(19,4))                          as ActualCostLR
   ,cast(ActualRevenueNLR as decimal(19,4))                      as ActualRevenueNLR
   ,cast(PlannedRevenueNLR as decimal(19,4))                     as PlannedRevenueNLR
   ,cast(FinancialStart as int)                                  as FinancialStart
   ,cast(FinancialEnd as int)                                    as FinancialEnd
   ,cast(FinancialOldStartDate as timestamp)                     as FinancialOldStartDate
   ,cast(RemainingForecastCostNLR as decimal(19,4))              as RemainingForecastCostNLR
   ,cast(RemainingForecastRevenueNLR as decimal(19,4))           as RemainingForecastRevenueNLR
   ,cast(CapexActualYTD as decimal(19,4))                        as CapexActualYTD
   ,cast(OpexActualYTD as decimal(19,4))                         as OpexActualYTD
   ,cast(CapexBudgetYTD as decimal(19,4))                        as CapexBudgetYTD
   ,cast(OpexBudgetYTD as decimal(19,4))                         as OpexBudgetYTD
   ,cast(FYForecastCostCapex as decimal(19,4))                   as FYForecastCostCapex
   ,cast(FYForecastCostOpex as decimal(19,4))                    as FYForecastCostOpex
   ,cast(FYForecastCost as decimal(19,4))                        as FYForecastCost
   ,cast(FYForecastatCompletionCost as decimal(19,4))            as FYForecastatCompletionCost
   ,cast(ProjectForecastatCompletionCost as decimal(19,4))       as ProjectForecastatCompletionCost
   ,cast(ActualBillableHours as decimal(19,4))                   as ActualBillableHours
   ,cast(ActualNonBillableHours as decimal(19,4))                as ActualNonBillableHours
   ,cast(ResourceRateDate as timestamp)                          as ResourceRateDate
   ,cast(LastReplanningDate as timestamp)                        as LastReplanningDate
   ,cast(FYBudgetedCost as decimal(19,4))                        as FYBudgetedCost
   ,cast(FYBudgetedCostCapex as decimal(19,4))                   as FYBudgetedCostCapex
   ,cast(FYBudgetedCostOpex as decimal(19,4))                    as FYBudgetedCostOpex
   ,cast(FYBudgetedCostNLR as decimal(19,4))                     as FYBudgetedCostNLR
   ,cast(FYBudgetedCostLR as decimal(19,4))                      as FYBudgetedCostLR
   ,cast(FYExpectedRevenue as decimal(19,4))                     as FYExpectedRevenue
   ,cast(FYForecastRevenue as decimal(19,4))                     as FYForecastRevenue
   ,cast(ExpectedRevenueLR as decimal(19,4))                     as ExpectedRevenueLR
   ,cast(FYExpectedRevenueLR as decimal(19,4))                   as FYExpectedRevenueLR
   ,cast(PlannedProfit as decimal(19,4))                         as PlannedProfit
   ,cast(FYPlannedProfit as decimal(19,4))                       as FYPlannedProfit
   ,cast(Phase as varchar(2000))                                 as Phase
   ,cast(Country as varchar(2000))                               as Country
   ,cast(C_Visibletocustomer as boolean)                         as C_Visibletocustomer
   ,C_TotalApprovedBudgetAllowance                               as C_TotalApprovedBudgetAllowance
-- ,cast(C_ActualAttendees as decimal(38,10))                    as C_ActualAttendees : Commented Out in RED
   ,C_TotalProjectedBudgetAllowance                              as C_TotalProjectedBudgetAllowance
   ,C_AnticipatedCosttoCompleteExternal                          as C_AnticipatedCosttoCompleteExternal
   ,C_TotalProjectedBudgetExternal                               as C_TotalProjectedBudgetExternal
   ,C_VarianceofBudgettoCostAllowance                            as C_VarianceofBudgettoCostAllowance
   ,C_VarianceofBudgettoCostExternal                             as C_VarianceofBudgettoCostExternal
   ,C_AnticipatedCosttoCompleteAllowance                         as C_AnticipatedCosttoCompleteAllowance
   ,C_TotalApprovedBudgetExternal                                as C_TotalApprovedBudgetExternal
   ,cast(C_AJD60 as varchar(2000))                               as C_AJD60
   ,cast(C_AJD59 as varchar(2000))                               as C_AJD59
   ,cast(C_AJD58 as varchar(2000))                               as C_AJD58
   ,cast(C_AJD57 as decimal(38,10))                              as C_AJD57
   ,cast(C_AJD56 as decimal(38,10))                              as C_AJD56
   ,cast(C_AJD55 as decimal(38,10))                              as C_AJD55
   ,cast(C_AJD54 as decimal(38,10))                              as C_AJD54
   ,cast(C_AJD53 as timestamp)                                   as C_AJD53
   ,cast(C_AJD52 as timestamp)                                   as C_AJD52
   ,cast(C_AJD51 as timestamp)                                   as C_AJD51
   ,cast(C_AJD50 as timestamp)                                   as C_AJD50
   ,cast(C_AJD49 as timestamp)                                   as C_AJD49
   ,cast(C_AJD48 as timestamp)                                   as C_AJD48
   ,cast(C_AJD47 as varchar(2000))                               as C_AJD47
   ,cast(C_AJD46 as varchar(2000))                               as C_AJD46
   ,cast(C_AJD45 as varchar(2000))                               as C_AJD45
   ,cast(C_AJD44 as varchar(2000))                               as C_AJD44
   ,cast(C_AJD43 as varchar(2000))                               as C_AJD43
   ,cast(C_AJD42 as varchar(2000))                               as C_AJD42
   ,cast(C_AJD41 as varchar(2000))                               as C_AJD41
   ,cast(C_AJD40 as varchar(2000))                               as C_AJD40
   ,cast(C_AJD39 as varchar(2000))                               as C_AJD39
   ,cast(C_AJD38 as varchar(2000))                               as C_AJD38
   ,cast(C_AJD37 as varchar(2000))                               as C_AJD37
   ,cast(C_AJD36 as varchar(2000))                               as C_AJD36
   ,cast(C_AJD35 as varchar(2000))                               as C_AJD35
   ,cast(C_AJD34 as varchar(2000))                               as C_AJD34
   ,cast(C_AJD33 as varchar(2000))                               as C_AJD33
   ,cast(C_AJD31 as varchar(2000))                               as C_AJD31
   ,cast(C_AJD32 as varchar(2000))                               as C_AJD32
-- ,cast(C_TotalInvoicesAllowanceRollup as decimal(19,4))        as C_TotalInvoicesAllowanceRollup : Commented Out in RED
   ,cast(C_TotalApprovedInvoicesRollup as decimal(19,4))         as C_TotalApprovedInvoicesRollup
   ,cast(C_TaxAmountRollup as decimal(19,4))                     as C_TaxAmountRollup
   ,cast(C_ReimbursableInvoiceAmountRollup as decimal(19,4))     as C_ReimbursableInvoiceAmountRollup
   ,cast(C_BaseInvoiceAmountRollup as decimal(19,4))             as C_BaseInvoiceAmountRollup
   ,cast(C_ApprovedChangesandTransfers as decimal(19,4))         as C_ApprovedChangesandTransfers
   ,cast(C_AdditionalForecastedCosts as decimal(19,4))           as C_AdditionalForecastedCosts
   ,cast(C_ApprovedChangesandTransferRollup as decimal(19,4))    as C_ApprovedChangesandTransferRollup
   ,cast(C_ApprovedCommitmentsAllowance as decimal(19,4))        as C_ApprovedCommitmentsAllowance
   ,cast(C_PendingCommitmentsAllowance as decimal(19,4))         as C_PendingCommitmentsAllowance
   ,cast(null as decimal(19,4))                                  as C_BaseInvoiceAmount
   ,cast(C_PendingChangesandTransfersRollup as decimal(19,4))    as C_PendingChangesandTransfersRollup
-- ,cast(C_ContractedAmount as decimal(19,4))                    as C_ContractedAmount : Commented Out in RED
   ,cast(C_AdditionalForecastedCostsRollup as decimal(19,4))     as C_AdditionalForecastedCostsRollup
   ,cast(C_OriginalBudgetRollup as decimal(19,4))                as C_OriginalBudgetRollup
   ,cast(C_ReportingId as varchar(2000))                         as C_ReportingId
   ,cast(C_PendingCommitmentsRollup as decimal(19,4))            as C_PendingCommitmentsRollup
   ,cast(null as decimal(19,4))                                  as C_ReimbursableInvoiceAmount
   ,cast(C_ApprovedCommitmentsRollup as decimal(19,4))           as C_ApprovedCommitmentsRollup
   ,cast(C_ReportingName as varchar(2000))                       as C_ReportingName
   ,cast(C_NextGenProgram as boolean)                            as C_NextGenProgram
   ,cast(C_RuntimeId as varchar(2000))                           as C_RuntimeId
   ,cast(C_CostType as varchar(2000))                            as C_CostType
   ,cast(C_MilestoneNumber as decimal(38,10))                    as C_MilestoneNumber
   ,cast(C_CostPerArea as decimal(19,4))                         as C_CostPerArea
   ,cast(C_ServiceProductE1Code as char(2000))                   as C_ServiceProductE1Code
   ,cast(C_ServiceProduct as varchar(2000))                      as C_ServiceProduct
   ,cast(C_AJD30 as varchar(2000))                               as C_AJD30
   ,cast(C_AJD29 as varchar(2000))                               as C_AJD29
   ,cast(C_AJD28 as varchar(2000))                               as C_AJD28
   ,cast(C_AJD26 as varchar(2000))                               as C_AJD26
   ,cast(C_AJD21 as char(2000))                                  as C_AJD21
   ,cast(C_AJD27 as varchar(2000))                               as C_AJD27
   ,cast(C_AJD25 as varchar(2000))                               as C_AJD25
   ,cast(C_AJD24 as varchar(2000))                               as C_AJD24
   ,cast(C_AJD23 as varchar(2000))                               as C_AJD23
   ,cast(C_AJD22 as varchar(2000))                               as C_AJD22
   ,cast(C_VarianceofBudgettoCost as decimal(19,4))              as C_VarianceofBudgettoCost
   ,cast(C_ProgramType as varchar(2000))                         as C_ProgramType
   ,cast(C_AJD20 as char(2000))                                  as C_AJD20
   ,cast(C_AJD19 as varchar(2000))                               as C_AJD19
   ,cast(C_AJD18 as char(2000))                                  as C_AJD18
   ,cast(C_AJD16 as varchar(2000))                               as C_AJD16
   ,cast(C_AJD17 as char(2000))                                  as C_AJD17
   ,cast(C_AJD15 as varchar(2000))                               as C_AJD15
   ,cast(C_AJD14 as varchar(2000))                               as C_AJD14
   ,cast(C_AJD13 as varchar(2000))                               as C_AJD13
   ,cast(C_AJD12 as varchar(2000))                               as C_AJD12
   ,cast(C_AJD11 as varchar(2000))                               as C_AJD11
   ,cast(C_AJD09 as varchar(2000))                               as C_AJD09
   ,cast(C_AJD10 as varchar(2000))                               as C_AJD10
   ,cast(C_AJD08 as varchar(2000))                               as C_AJD08
   ,cast(C_AJD07 as varchar(2000))                               as C_AJD07
   ,cast(C_ScopeStatus as varchar(2000))                         as C_ScopeStatus
   ,cast(C_ScheduleStatus as varchar(2000))                      as C_ScheduleStatus
   ,cast(C_AnticipatedCosttoComplete as decimal(19,4))           as C_AnticipatedCosttoComplete
   ,cast(C_TotalProjectedBudget as decimal(19,4))                as C_TotalProjectedBudget
   ,cast(C_RemainingBalancetoInvoice as decimal(19,4))           as C_RemainingBalancetoInvoice
   ,cast(C_AJD06 as varchar(2000))                               as C_AJD06
   ,cast(C_AJD05 as varchar(2000))                               as C_AJD05
   ,cast(C_AJD04 as varchar(2000))                               as C_AJD04
   ,cast(C_AJD03 as varchar(2000))                               as C_AJD03
   ,cast(C_AJD02 as varchar(2000))                               as C_AJD02
   ,cast(C_AJD01 as varchar(2000))                               as C_AJD01
   ,cast(C_RP30 as varchar(2000))                                as C_RP30
   ,cast(C_RP29 as varchar(2000))                                as C_RP29
   ,cast(C_RP28 as varchar(2000))                                as C_RP28
   ,cast(C_RP27 as varchar(2000))                                as C_RP27
   ,cast(C_RP26 as varchar(2000))                                as C_RP26
   ,cast(C_TotalPendingInvoices as decimal(19,4))                as C_TotalPendingInvoices
   ,cast(C_TotalInvoicesAllowance as decimal(19,4))              as C_TotalInvoicesAllowance
   ,cast(C_ProjectedTotalCommitments as decimal(19,4))           as C_ProjectedTotalCommitments
   ,cast(C_RiskRegistryScore as decimal(38,10))                  as C_RiskRegistryScore
   ,cast(C_RiskRegisterStatus as varchar(2000))                  as C_RiskRegisterStatus
   ,cast(C_ProjectProgramforMeetingMinutes as varchar(200))      as C_ProjectProgramforMeetingMinutes
   ,cast(C_RP23 as varchar(2000))                                as C_RP23
   ,cast(C_RP13 as char(2000))                                   as C_RP13
   ,cast(C_RP12 as varchar(2000))                                as C_RP12
   ,cast(C_RP10 as varchar(2000))                                as C_RP10
   ,cast(C_RP09 as varchar(2000))                                as C_RP09
   ,cast(C_OriginalBudgetExternal as decimal(19,4))              as C_OriginalBudgetExternal
   ,cast(C_TotalInvoicesExternal as decimal(19,4))               as C_TotalInvoicesExternal
   ,cast(C_PendingCommitmentsExternal as decimal(19,4))          as C_PendingCommitmentsExternal
   ,cast(C_ApprovedCommitmentsExternal as decimal(19,4))         as C_ApprovedCommitmentsExternal
   ,cast(C_PendingChangesandTransfersExternal as decimal(19,4))  as C_PendingChangesandTransfersExternal
   ,cast(C_ApprovedChangesandTransfersExternal as decimal(19,4)) as C_ApprovedChangesandTransfersExternal
   ,cast(C_TotalInvoices as decimal(19,4))                       as C_TotalInvoices
   ,cast(C_TotalApprovedBudget as decimal(19,4))                 as C_TotalApprovedBudget
   ,cast(C_TotalApprovedInvoices as decimal(19,4))               as C_TotalApprovedInvoices
   ,cast(C_RP08 as varchar(2000))                                as C_RP08
   ,cast(C_AdditionalForecastedCostsExternal as decimal(19,4))   as C_AdditionalForecastedCostsExternal
   ,cast(C_ApprovedCommitments as decimal(19,4))                 as C_ApprovedCommitments
   ,cast(C_OriginalBudgetAllowance as decimal(19,4))             as C_OriginalBudgetAllowance
   ,cast(C_PendingChangesandTransfers as decimal(19,4))          as C_PendingChangesandTransfers
   ,cast(C_OriginalBudget as decimal(19,4))                      as C_OriginalBudget
   ,cast(C_PendingCommitments as decimal(19,4))                  as C_PendingCommitments
   ,cast(C_Vendor as varchar(200))                               as C_Vendor
   ,cast(C_InternalType as varchar(2000))                        as C_InternalType
   ,cast(C_ProjectBuildingSize as decimal(38,10))                as C_ProjectBuildingSize
   ,cast(C_CAPEXP as varchar(2000))                              as C_CAPEXP
   ,cast(C_Customertask as boolean)                              as C_Customertask
 --  ,cast(dss_file_path as varchar(500))                          as dss_file_path
   ,''                                                           as dss_file_path
--   ,cast(dss_loaded_at as timestamp)                             as dss_loaded_at
   ,''                                                           as dss_loaded_at  
   ,''                                                           as C_OOTB
   ,''                                                           as C_ProjectedEndDate
   ,''                                                           as C_CostPerAreaDEPRECATED
   ,''                                                           as C_CostCode
   ,''                                                           as C_AdditionalForecastedCostsCap
   ,''                                                           as C_AdditionalForecastedCostsExp
   ,''                                                           as C_AnticipatedCosttoCompleteCap
   ,''                                                           as C_AnticipatedCosttoCompleteExp
   ,''                                                           as C_ApprovedChangesandTransfersCap
   ,''                                                           as C_ApprovedChangesandTransfersExp
   ,''                                                           as C_ApprovedCommitmentsCap
   ,''                                                           as C_ApprovedCommitmentsExp
   ,''                                                           as C_OriginalBudgetCap
   ,''                                                           as C_OriginalBudgetExp
   ,''                                                           as C_PendingChangesandTransfersCap
   ,''                                                           as C_PendingChangesandTransfersExp
   ,''                                                           as C_PendingCommitmentsCap
   ,''                                                           as C_PendingCommitmentsExp
   ,''                                                           as C_ProjectedTotalCommitmentsCap
   ,''                                                           as C_ProjectedTotalCommitmentsExp
   ,''                                                           as C_RemainingBalancetoInvoiceCap
   ,''                                                           as C_RemainingBalancetoInvoiceExp
   ,''                                                           as C_TotalApprovedBudgetCap
   ,''                                                           as C_TotalApprovedBudgetExp
   ,''                                                           as C_TotalApprovedInvoicesCap
   ,''                                                           as C_TotalApprovedInvoicesExp
   ,''                                                           as C_TotalInvoicesCap
   ,''                                                           as C_TotalInvoicesExp
   ,''                                                           as C_TotalPendingInvoicesCap
   ,''                                                           as C_TotalPendingInvoicesExp
   ,''                                                           as C_TotalProjectedBudgetCap
   ,''                                                           as C_TotalProjectedBudgetExp
   ,''                                                           as C_VarianceofBudgettoCostCap
   ,''                                                           as C_VarianceofBudgettoCostExp
   ,C_ClientAssetValues                                          as C_ClientAssetValues
   ,C_ClientGrouping                                             as C_ClientGrouping
   ,C_ClientWBSCode                                              as C_ClientWBSCode
   ,''                                                           as C_VendorChangeOrderCost
   ,''                                                           as C_VendorOriginalCost
   ,''                                                           as C_CostCodeName
   ,''                                                           as C_Financial_Complete
   ,''                                                           as C_PackageType
from {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project

union all

 SELECT
	cast(batch_id as char(32))                                   as batch_id
   ,cast(batch_timestamp as timestamp)                           as batch_timestamp
   ,cast(is_deleted as boolean)                                  as is_deleted
   ,cast(id as varchar(100))                                     as id
   ,cast(CreatedBy as varchar(200))                              as CreatedBy
   ,cast(CreatedOn as timestamp)                                 as CreatedOn
   ,cast(LastUpdatedBy as varchar(200))                          as LastUpdatedBy
   ,cast(LastUpdatedOn as timestamp)                             as LastUpdatedOn
   ,cast(Name as varchar(500))                                   as Name
   ,cast(Description as varchar(2000))                           as Description
   ,cast(ExternalID as varchar(1950))                            as ExternalID
   ,cast(State as varchar(2000))                                 as State
   ,cast(StartDate as timestamp)                                 as StartDate
   ,cast(DueDate as timestamp)                                   as DueDate
   ,cast(Duration as decimal(19,4))                              as Duration
   ,cast(ActualStartDate as timestamp)                           as ActualStartDate
   ,cast(ActualEndDate as timestamp)                             as ActualEndDate
   ,cast(TrackStatus as varchar(2000))                           as TrackStatus
   ,cast(Conflicts as decimal(19,4))                             as Conflicts
   ,cast(OnCriticalPath as boolean)                              as OnCriticalPath
   ,cast(DurationManuallySet as boolean)                         as DurationManuallySet
   ,cast(TrackStatusManuallySet as boolean)                      as TrackStatusManuallySet
   ,cast(EarliestStartDate as timestamp)                         as EarliestStartDate
   ,cast(LatestStartDate as timestamp)                           as LatestStartDate
   ,cast(EarliestEndDate as timestamp)                           as EarliestEndDate
   ,cast(LatestEndDate as timestamp)                             as LatestEndDate
   ,cast(ExpectedProgress as decimal(38,10))                     as ExpectedProgress
   ,cast(SchedulingType as varchar(2000))                        as SchedulingType
   ,cast(ImportedFrom as varchar(2000))                          as ImportedFrom
   ,cast(Importance as varchar(2000))                            as Importance
   ,cast(Priority as int)                                        as Priority
   ,cast(PercentCompleted as decimal(38,10))                     as PercentCompleted
   ,cast(Manager as varchar(200))                                as Manager
   ,cast(ChargedTypeManuallySet as boolean)                      as ChargedTypeManuallySet
   ,cast(ChildrenCount as int)                                   as ChildrenCount
   ,cast(SuccessorsCount as int)                                 as SuccessorsCount
   ,cast(PredecessorsCount as int)                               as PredecessorsCount
   ,cast(AlluserResourcesCount as int)                           as AlluserResourcesCount
   ,cast(AttachmentsCount as int)                                as AttachmentsCount
   ,cast(PostsCount as int)                                      as PostsCount
   ,cast(NotesCount as int)                                      as NotesCount
   ,cast(Reportable as boolean)                                  as Reportable
   ,cast(ReportableManuallySet as boolean)                       as ReportableManuallySet
   ,cast(Billable as boolean)                                    as Billable
   ,cast(ChildShortcutCount as int)                              as ChildShortcutCount
   ,cast(Project as varchar(200))                                as Project
   ,cast(WorkPolicy as varchar(2000))                            as WorkPolicy
   ,cast(CommitLevel as varchar(2000))                           as CommitLevel
   ,cast(ReportingStartDate as timestamp)                        as ReportingStartDate
   ,cast(SYSID as varchar(40))                                   as SYSID
   ,cast(Work as decimal(19,4))                                  as Work
   ,cast(ActualEffort as decimal(19,4))                          as ActualEffort
   ,cast(RemainingEffort as decimal(19,4))                       as RemainingEffort
   ,cast(WorkManuallySet as boolean)                             as WorkManuallySet
   ,cast(RemainingEffortManuallySet as boolean)                  as RemainingEffortManuallySet
--    ,cast(WorkVariance as decimal(19,4))                          as WorkVariance
    ,null                                                       as WorkVariance
   ,cast(ActualDuration as decimal(19,4))                        as ActualDuration
   ,cast(StartDateVariance as decimal(19,4))                     as StartDateVariance
   ,cast(ActualCost as decimal(19,4))                            as ActualCost
   ,cast(DueDateVariance as decimal(19,4))                       as DueDateVariance
   ,cast(PlannedBudget as decimal(19,4))                         as PlannedBudget
   ,cast(DurationVariance as decimal(19,4))                      as DurationVariance
   ,cast(ActualCostManuallySet as boolean)                       as ActualCostManuallySet
   ,cast(PlannedBudgetManuallySet as boolean)                    as PlannedBudgetManuallySet
   ,cast(TimeTrackingEffort as decimal(19,4))                    as TimeTrackingEffort
   ,cast(TimeTrackingCost as decimal(19,4))                      as TimeTrackingCost
   ,cast(FixedCost as decimal(19,4))                             as FixedCost
   ,cast(FixedPrice as decimal(19,4))                            as FixedPrice
   ,cast(PercentInvested as decimal(38,10))                      as PercentInvested
   ,cast(CostVariance as decimal(19,4))                          as CostVariance
   ,cast(TimeTrackingBilling as decimal(19,4))                   as TimeTrackingBilling
   ,cast(EarnedValue as decimal(19,4))                           as EarnedValue
   ,cast(PlannedRevenue as decimal(19,4))                        as PlannedRevenue
   ,cast(CPI as decimal(38,10))                                  as CPI
   ,cast(ActualRevenue as decimal(19,4))                         as ActualRevenue
   ,cast(SPI as decimal(38,10))                                  as SPI
   ,cast(PlannedRevenueManuallySet as boolean)                   as PlannedRevenueManuallySet
   ,cast(ActualRevenueManuallySet as boolean)                    as ActualRevenueManuallySet
   ,cast(Profitability as decimal(19,4))                         as Profitability
   ,cast(PercentProfitability as decimal(38,10))                 as PercentProfitability
   ,cast(PlannedExpenses as decimal(19,4))                       as PlannedExpenses
   ,cast(DirectPlannedExpenses as decimal(19,4))                 as DirectPlannedExpenses
   ,cast(ActualExpenses as decimal(19,4))                        as ActualExpenses
   ,cast(DirectActualExpenses as decimal(19,4))                  as DirectActualExpenses
   ,cast(ProjectedExpenses as decimal(19,4))                     as ProjectedExpenses
   ,cast(DirectProjectedExpenses as decimal(19,4))               as DirectProjectedExpenses
   ,cast(PlannedBilledExpenses as decimal(19,4))                 as PlannedBilledExpenses
   ,cast(DirectPlannedBilledExpenses as decimal(19,4))           as DirectPlannedBilledExpenses
   ,cast(ActualBilledExpenses as decimal(19,4))                  as ActualBilledExpenses
   ,cast(DirectActualBilledExpenses as decimal(19,4))            as DirectActualBilledExpenses
   ,cast(ProjectedBilledExpenses as decimal(19,4))               as ProjectedBilledExpenses
   ,cast(DirectProjectedBilledExpenses as decimal(19,4))         as DirectProjectedBilledExpenses
   ,cast(RevenueCurrencyType as varchar(2000))                   as RevenueCurrencyType
   ,cast(CostCurrencyType as varchar(2000))                      as CostCurrencyType
   ,cast(Pending as varchar(2000))                               as Pending
   ,cast(IssuesCount as int)                                     as IssuesCount
   ,cast(LastUpdatedBySystemOn as timestamp)                     as LastUpdatedBySystemOn
   ,cast(AllowReportingOnSubItems as boolean)                    as AllowReportingOnSubItems
   ,cast(CommittedDate as timestamp)                             as CommittedDate
   ,cast(userResourcesCount as int)                              as userResourcesCount
   ,cast(BudgetedHours as decimal(19,4))                         as BudgetedHours
   ,cast(EmailsCount as int)                                     as EmailsCount
   ,cast(CostBalance as decimal(19,4))                           as CostBalance
   ,cast(BudgetStatus as varchar(2000))                          as BudgetStatus
   ,cast(RevenueBalance as decimal(19,4))                        as RevenueBalance
   ,cast(ActualEffortUpdatedFromTimesheets as boolean)           as ActualEffortUpdatedFromTimesheets
   ,cast(ParentProject as varchar(200))                          as ParentProject
   ,cast(SfExternalId as varchar(256))                           as SfExternalId
   ,cast(SfExternalName as varchar(256))                         as SfExternalName
   ,cast(InternalId as varchar(512))                             as InternalId
   ,cast(OrderID as varchar(512))                                as OrderID
   ,cast(SKU as varchar(512))                                    as SKU
   ,cast(BaselineStartDate as timestamp)                         as BaselineStartDate
   ,cast(BaselineStartDateVariance as decimal(19,4))             as BaselineStartDateVariance
   ,cast(BaselineDueDate as timestamp)                           as BaselineDueDate
   ,cast(BaselineDueDateVariance as decimal(19,4))               as BaselineDueDateVariance
   ,cast(BaselineDuration as decimal(19,4))                      as BaselineDuration
   ,cast(BaselineDurationVariance as decimal(19,4))              as BaselineDurationVariance
   ,cast(BaselineWork as decimal(19,4))                          as BaselineWork
   ,cast(BaselineWorkVariance as decimal(19,4))                  as BaselineWorkVariance
   ,cast(BaselineCost as decimal(19,4))                          as BaselineCost
   ,cast(BaselineCostsVariance as decimal(19,4))                 as BaselineCostsVariance
   ,cast(BaselineRevenue as decimal(19,4))                       as BaselineRevenue
   ,cast(BaselineRevenueVariance as decimal(19,4))               as BaselineRevenueVariance
   ,cast(InternalStatus as varchar(512))                         as InternalStatus
   ,cast(Deliverable as boolean)                                 as Deliverable
   ,cast(DeliverableType as varchar(512))                        as DeliverableType
   ,cast(Executable as boolean)                                  as Executable
   ,cast(Parent as varchar(200))                                 as Parent
   ,cast(PlannedAmount as decimal(19,4))                         as PlannedAmount
   ,cast(ChargedAmount as decimal(19,4))                         as ChargedAmount
   ,cast(TCPI as decimal(38,10))                                 as TCPI
   ,cast(TotalEstimatedCost as decimal(19,4))                    as TotalEstimatedCost
   ,cast(Charged as varchar(2000))                               as Charged
   ,cast(ChargedAmountManuallySet as boolean)                    as ChargedAmountManuallySet
   ,cast(RevenueEarnedValue as decimal(19,4))                    as RevenueEarnedValue
   ,cast(RPI as decimal(38,10))                                  as RPI
   ,cast(RTCPI as decimal(38,10))                                as RTCPI
   ,cast(ReportableStartDate as timestamp)                       as ReportableStartDate
   ,cast(EntityType as varchar(200))                             as EntityType
   ,cast(CompletnessDefinition as decimal(19,4))                 as CompletnessDefinition
   ,cast(ReportableEndDate as timestamp)                         as ReportableEndDate
   ,cast(TaskReportingPolicy as varchar(2000))                   as TaskReportingPolicy
   ,cast(TaskReportingPolicyManuallySet as boolean)              as TaskReportingPolicyManuallySet
   ,cast(FloatingTask as boolean)                                as FloatingTask
   ,cast(IndividualReporting as boolean)                         as IndividualReporting
   ,cast(BaselineCreationDate as timestamp)                      as BaselineCreationDate
   ,cast(AggregatedStopwatchesCount as int)                      as AggregatedStopwatchesCount
   ,cast(CalculateCompletenessBasedOnEfforts as boolean)         as CalculateCompletenessBasedOnEfforts
   ,cast(ActiveStopwatch as varchar(2000))                       as ActiveStopwatch
   ,cast(ObjectAlias as varchar(1000))                           as ObjectAlias
   ,cast(StopwatchesCount as int)                                as StopwatchesCount
   ,cast(LikesCount as int)                                      as LikesCount
   ,cast(BudgetedHoursManuallySet as boolean)                    as BudgetedHoursManuallySet
   ,cast(CurrencyEAC as decimal(28,4))                           as CurrencyEAC
   ,cast(PendingTimeTrackingEffort as decimal(19,4))             as PendingTimeTrackingEffort
   ,cast(CurrencyREAC as decimal(19,4))                          as CurrencyREAC
   ,cast(CurrencyETC as decimal(28,4))                           as CurrencyETC
   ,cast(ImageUrl as varchar(256))                               as ImageUrl
   ,cast(CurrencyRETC as decimal(19,4))                          as CurrencyRETC
   ,cast(SetByLeveling as decimal(19,4))                         as SetByLeveling
   ,cast(GeographicalRegion as varchar(2000))                    as GeographicalRegion
   ,cast(ResourceUtilizationCategory as varchar(2000))           as ResourceUtilizationCategory
   ,cast(StateProvince as varchar(2000))                         as StateProvince
   ,cast(ExpenseType as varchar(2000))                           as ExpenseType
   ,cast(BudgetCostOPEX as decimal(19,4))                        as BudgetCostOPEX
   ,cast(BudgetCostCAPEX as decimal(19,4))                       as BudgetCostCAPEX
   ,cast(BudgetCostNLR as decimal(19,4))                         as BudgetCostNLR
   ,cast(BudgetCostLR as decimal(19,4))                          as BudgetCostLR
   ,cast(EntityOwner as varchar(200))                            as EntityOwner
   ,cast(ActualCostOPEX as decimal(19,4))                        as ActualCostOPEX
   ,cast(ActualCostCAPEX as decimal(19,4))                       as ActualCostCAPEX
   ,cast(ActualCostNLR as decimal(19,4))                         as ActualCostNLR
   ,cast(ActualCostNLROpex as decimal(19,4))                     as ActualCostNLROpex
   ,cast(ActualCostNLRCapex as decimal(19,4))                    as ActualCostNLRCapex
   ,cast(ActualCostLR as decimal(19,4))                          as ActualCostLR
   ,cast(ActualRevenueNLR as decimal(19,4))                      as ActualRevenueNLR
   ,cast(PlannedRevenueNLR as decimal(19,4))                     as PlannedRevenueNLR
   ,cast(FinancialStart as int)                                  as FinancialStart
   ,cast(FinancialEnd as int)                                    as FinancialEnd
   ,cast(FinancialOldStartDate as timestamp)                     as FinancialOldStartDate
   ,cast(RemainingForecastCostNLR as decimal(19,4))              as RemainingForecastCostNLR
   ,cast(RemainingForecastRevenueNLR as decimal(19,4))           as RemainingForecastRevenueNLR
   ,cast(CapexActualYTD as decimal(19,4))                        as CapexActualYTD
   ,cast(OpexActualYTD as decimal(19,4))                         as OpexActualYTD
   ,cast(CapexBudgetYTD as decimal(19,4))                        as CapexBudgetYTD
   ,cast(OpexBudgetYTD as decimal(19,4))                         as OpexBudgetYTD
   ,cast(FYForecastCostCapex as decimal(19,4))                   as FYForecastCostCapex
   ,cast(FYForecastCostOpex as decimal(19,4))                    as FYForecastCostOpex
   ,cast(FYForecastCost as decimal(19,4))                        as FYForecastCost
   ,cast(FYForecastatCompletionCost as decimal(19,4))            as FYForecastatCompletionCost
   ,cast(ProjectForecastatCompletionCost as decimal(19,4))       as ProjectForecastatCompletionCost
   ,cast(ActualBillableHours as decimal(19,4))                   as ActualBillableHours
   ,cast(ActualNonBillableHours as decimal(19,4))                as ActualNonBillableHours
   ,cast(ResourceRateDate as timestamp)                          as ResourceRateDate
   ,cast(LastReplanningDate as timestamp)                        as LastReplanningDate
   ,cast(FYBudgetedCost as decimal(19,4))                        as FYBudgetedCost
   ,cast(FYBudgetedCostCapex as decimal(19,4))                   as FYBudgetedCostCapex
   ,cast(FYBudgetedCostOpex as decimal(19,4))                    as FYBudgetedCostOpex
   ,cast(FYBudgetedCostNLR as decimal(19,4))                     as FYBudgetedCostNLR
   ,cast(FYBudgetedCostLR as decimal(19,4))                      as FYBudgetedCostLR
   ,cast(FYExpectedRevenue as decimal(19,4))                     as FYExpectedRevenue
   ,cast(FYForecastRevenue as decimal(19,4))                     as FYForecastRevenue
   ,cast(ExpectedRevenueLR as decimal(19,4))                     as ExpectedRevenueLR
   ,cast(FYExpectedRevenueLR as decimal(19,4))                   as FYExpectedRevenueLR
   ,cast(PlannedProfit as decimal(19,4))                         as PlannedProfit
   ,cast(FYPlannedProfit as decimal(19,4))                       as FYPlannedProfit
   ,cast(Phase as varchar(2000))                                 as Phase
   ,cast(Country as varchar(2000))                               as Country
   ,cast(C_Visibletocustomer as boolean)                         as C_Visibletocustomer
   ,C_TotalApprovedBudgetAllowance                               as C_TotalApprovedBudgetAllowance
-- ,cast(C_ActualAttendees as decimal(38,10))                    as C_ActualAttendees : Commented Out in RED
   ,C_TotalProjectedBudgetAllowance                              as C_TotalProjectedBudgetAllowance
   ,C_AnticipatedCosttoCompleteExternal                          as C_AnticipatedCosttoCompleteExternal
   ,C_TotalProjectedBudgetExternal                               as C_TotalProjectedBudgetExternal
   ,C_VarianceofBudgettoCostAllowance                            as C_VarianceofBudgettoCostAllowance
   ,C_VarianceofBudgettoCostExternal                             as C_VarianceofBudgettoCostExternal
   ,C_AnticipatedCosttoCompleteAllowance                         as C_AnticipatedCosttoCompleteAllowance
   ,C_TotalApprovedBudgetExternal                                as C_TotalApprovedBudgetExternal
   ,cast(C_AJD60 as varchar(2000))                               as C_AJD60
   ,cast(C_AJD59 as varchar(2000))                               as C_AJD59
   ,cast(C_AJD58 as varchar(2000))                               as C_AJD58
   ,cast(C_AJD57 as decimal(38,10))                              as C_AJD57
   ,cast(C_AJD56 as decimal(38,10))                              as C_AJD56
   ,cast(C_AJD55 as decimal(38,10))                              as C_AJD55
   ,cast(C_AJD54 as decimal(38,10))                              as C_AJD54
   ,cast(C_AJD53 as timestamp)                                   as C_AJD53
   ,cast(C_AJD52 as timestamp)                                   as C_AJD52
   ,cast(C_AJD51 as timestamp)                                   as C_AJD51
   ,cast(C_AJD50 as timestamp)                                   as C_AJD50
   ,cast(C_AJD49 as timestamp)                                   as C_AJD49
   ,cast(C_AJD48 as timestamp)                                   as C_AJD48
   ,cast(C_AJD47 as varchar(2000))                               as C_AJD47
   ,cast(C_AJD46 as varchar(2000))                               as C_AJD46
   ,cast(C_AJD45 as varchar(2000))                               as C_AJD45
   ,cast(C_AJD44 as varchar(2000))                               as C_AJD44
   ,cast(C_AJD43 as varchar(2000))                               as C_AJD43
   ,cast(C_AJD42 as varchar(2000))                               as C_AJD42
   ,cast(C_AJD41 as varchar(2000))                               as C_AJD41
   ,cast(C_AJD40 as varchar(2000))                               as C_AJD40
   ,cast(C_AJD39 as varchar(2000))                               as C_AJD39
   ,cast(C_AJD38 as varchar(2000))                               as C_AJD38
   ,cast(C_AJD37 as varchar(2000))                               as C_AJD37
   ,cast(C_AJD36 as varchar(2000))                               as C_AJD36
   ,cast(C_AJD35 as varchar(2000))                               as C_AJD35
   ,cast(C_AJD34 as varchar(2000))                               as C_AJD34
   ,cast(C_AJD33 as varchar(2000))                               as C_AJD33
   ,cast(C_AJD31 as varchar(2000))                               as C_AJD31
   ,cast(C_AJD32 as varchar(2000))                               as C_AJD32
-- ,cast(C_TotalInvoicesAllowanceRollup as decimal(19,4))        as C_TotalInvoicesAllowanceRollup : Commented Out in RED
   ,cast(C_TotalApprovedInvoicesRollup as decimal(19,4))         as C_TotalApprovedInvoicesRollup
   ,cast(C_TaxAmountRollup as decimal(19,4))                     as C_TaxAmountRollup
   ,cast(C_ReimbursableInvoiceAmountRollup as decimal(19,4))     as C_ReimbursableInvoiceAmountRollup
   ,cast(C_BaseInvoiceAmountRollup as decimal(19,4))             as C_BaseInvoiceAmountRollup
   ,cast(C_ApprovedChangesandTransfers as decimal(19,4))         as C_ApprovedChangesandTransfers
   ,cast(C_AdditionalForecastedCosts as decimal(19,4))           as C_AdditionalForecastedCosts
   ,cast(C_ApprovedChangesandTransferRollup as decimal(19,4))    as C_ApprovedChangesandTransferRollup
   ,cast(C_ApprovedCommitmentsAllowance as decimal(19,4))        as C_ApprovedCommitmentsAllowance
   ,cast(C_PendingCommitmentsAllowance as decimal(19,4))         as C_PendingCommitmentsAllowance
   ,cast(null as decimal(19,4))                                  as C_BaseInvoiceAmount
   ,cast(C_PendingChangesandTransfersRollup as decimal(19,4))    as C_PendingChangesandTransfersRollup
-- ,cast(C_ContractedAmount as decimal(19,4))                    as C_ContractedAmount : Commented Out in RED
   ,cast(C_AdditionalForecastedCostsRollup as decimal(19,4))     as C_AdditionalForecastedCostsRollup
   ,cast(C_OriginalBudgetRollup as decimal(19,4))                as C_OriginalBudgetRollup
   ,cast(C_ReportingId as varchar(2000))                         as C_ReportingId
   ,cast(C_PendingCommitmentsRollup as decimal(19,4))            as C_PendingCommitmentsRollup
   ,cast(null as decimal(19,4))                                  as C_ReimbursableInvoiceAmount
   ,cast(C_ApprovedCommitmentsRollup as decimal(19,4))           as C_ApprovedCommitmentsRollup
   ,cast(C_ReportingName as varchar(2000))                       as C_ReportingName
   ,cast(C_NextGenProgram as boolean)                            as C_NextGenProgram
   ,cast(C_RuntimeId as varchar(2000))                           as C_RuntimeId
   ,cast(C_CostType as varchar(2000))                            as C_CostType
   ,cast(C_MilestoneNumber as decimal(38,10))                    as C_MilestoneNumber
   ,cast(C_CostPerArea as decimal(19,4))                         as C_CostPerArea
   ,cast(C_ServiceProductE1Code as char(2000))                   as C_ServiceProductE1Code
   ,cast(C_ServiceProduct as varchar(2000))                      as C_ServiceProduct
   ,cast(C_AJD30 as varchar(2000))                               as C_AJD30
   ,cast(C_AJD29 as varchar(2000))                               as C_AJD29
   ,cast(C_AJD28 as varchar(2000))                               as C_AJD28
   ,cast(C_AJD26 as varchar(2000))                               as C_AJD26
   ,cast(C_AJD21 as char(2000))                                  as C_AJD21
   ,cast(C_AJD27 as varchar(2000))                               as C_AJD27
   ,cast(C_AJD25 as varchar(2000))                               as C_AJD25
   ,cast(C_AJD24 as varchar(2000))                               as C_AJD24
   ,cast(C_AJD23 as varchar(2000))                               as C_AJD23
   ,cast(C_AJD22 as varchar(2000))                               as C_AJD22
   ,cast(C_VarianceofBudgettoCost as decimal(19,4))              as C_VarianceofBudgettoCost
   ,cast(C_ProgramType as varchar(2000))                         as C_ProgramType
   ,cast(C_AJD20 as char(2000))                                  as C_AJD20
   ,cast(C_AJD19 as varchar(2000))                               as C_AJD19
   ,cast(C_AJD18 as char(2000))                                  as C_AJD18
   ,cast(C_AJD16 as varchar(2000))                               as C_AJD16
   ,cast(C_AJD17 as char(2000))                                  as C_AJD17
   ,cast(C_AJD15 as varchar(2000))                               as C_AJD15
   ,cast(C_AJD14 as varchar(2000))                               as C_AJD14
   ,cast(C_AJD13 as varchar(2000))                               as C_AJD13
   ,cast(C_AJD12 as varchar(2000))                               as C_AJD12
   ,cast(C_AJD11 as varchar(2000))                               as C_AJD11
   ,cast(C_AJD09 as varchar(2000))                               as C_AJD09
   ,cast(C_AJD10 as varchar(2000))                               as C_AJD10
   ,cast(C_AJD08 as varchar(2000))                               as C_AJD08
   ,cast(C_AJD07 as varchar(2000))                               as C_AJD07
   ,cast(C_ScopeStatus as varchar(2000))                         as C_ScopeStatus
   ,cast(C_ScheduleStatus as varchar(2000))                      as C_ScheduleStatus
   ,cast(C_AnticipatedCosttoComplete as decimal(19,4))           as C_AnticipatedCosttoComplete
   ,cast(C_TotalProjectedBudget as decimal(19,4))                as C_TotalProjectedBudget
   ,cast(C_RemainingBalancetoInvoice as decimal(19,4))           as C_RemainingBalancetoInvoice
   ,cast(C_AJD06 as varchar(2000))                               as C_AJD06
   ,cast(C_AJD05 as varchar(2000))                               as C_AJD05
   ,cast(C_AJD04 as varchar(2000))                               as C_AJD04
   ,cast(C_AJD03 as varchar(2000))                               as C_AJD03
   ,cast(C_AJD02 as varchar(2000))                               as C_AJD02
   ,cast(C_AJD01 as varchar(2000))                               as C_AJD01
   ,cast(C_RP30 as varchar(2000))                                as C_RP30
   ,cast(C_RP29 as varchar(2000))                                as C_RP29
   ,cast(C_RP28 as varchar(2000))                                as C_RP28
   ,cast(C_RP27 as varchar(2000))                                as C_RP27
   ,cast(C_RP26 as varchar(2000))                                as C_RP26
   ,cast(C_TotalPendingInvoices as decimal(19,4))                as C_TotalPendingInvoices
   ,cast(C_TotalInvoicesAllowance as decimal(19,4))              as C_TotalInvoicesAllowance
   ,cast(C_ProjectedTotalCommitments as decimal(19,4))           as C_ProjectedTotalCommitments
   ,cast(C_RiskRegistryScore as decimal(38,10))                  as C_RiskRegistryScore
   ,cast(C_RiskRegisterStatus as varchar(2000))                  as C_RiskRegisterStatus
   ,cast(C_ProjectProgramforMeetingMinutes as varchar(200))      as C_ProjectProgramforMeetingMinutes
   ,cast(C_RP23 as varchar(2000))                                as C_RP23
   ,cast(C_RP13 as char(2000))                                   as C_RP13
   ,cast(C_RP12 as varchar(2000))                                as C_RP12
   ,cast(C_RP10 as varchar(2000))                                as C_RP10
   ,cast(C_RP09 as varchar(2000))                                as C_RP09
   ,cast(C_OriginalBudgetExternal as decimal(19,4))              as C_OriginalBudgetExternal
   ,cast(C_TotalInvoicesExternal as decimal(19,4))               as C_TotalInvoicesExternal
   ,cast(C_PendingCommitmentsExternal as decimal(19,4))          as C_PendingCommitmentsExternal
   ,cast(C_ApprovedCommitmentsExternal as decimal(19,4))         as C_ApprovedCommitmentsExternal
   ,cast(C_PendingChangesandTransfersExternal as decimal(19,4))  as C_PendingChangesandTransfersExternal
   ,cast(C_ApprovedChangesandTransfersExternal as decimal(19,4)) as C_ApprovedChangesandTransfersExternal
   ,cast(C_TotalInvoices as decimal(19,4))                       as C_TotalInvoices
   ,cast(C_TotalApprovedBudget as decimal(19,4))                 as C_TotalApprovedBudget
   ,cast(C_TotalApprovedInvoices as decimal(19,4))               as C_TotalApprovedInvoices
   ,cast(C_RP08 as varchar(2000))                                as C_RP08
   ,cast(C_AdditionalForecastedCostsExternal as decimal(19,4))   as C_AdditionalForecastedCostsExternal
   ,cast(C_ApprovedCommitments as decimal(19,4))                 as C_ApprovedCommitments
   ,cast(C_OriginalBudgetAllowance as decimal(19,4))             as C_OriginalBudgetAllowance
   ,cast(C_PendingChangesandTransfers as decimal(19,4))          as C_PendingChangesandTransfers
   ,cast(C_OriginalBudget as decimal(19,4))                      as C_OriginalBudget
   ,cast(C_PendingCommitments as decimal(19,4))                  as C_PendingCommitments
   ,cast(C_Vendor as varchar(200))                               as C_Vendor
   ,cast(C_InternalType as varchar(2000))                        as C_InternalType
   ,cast(C_ProjectBuildingSize as decimal(38,10))                as C_ProjectBuildingSize
   ,cast(C_CAPEXP as varchar(2000))                              as C_CAPEXP
   ,cast(C_Customertask as boolean)                              as C_Customertask
   ,''                                                           as dss_file_path -- DNE in Azara Snapshot Table
   ,''                                                           as dss_loaded_at -- DNE in Azara Snapshot Table
   ,''                                                           as C_OOTB
   ,''                                                           as C_ProjectedEndDate
   ,''                                                           as C_CostPerAreaDEPRECATED
   ,''                                                           as C_CostCode
   ,''                                                           as C_AdditionalForecastedCostsCap
   ,''                                                           as C_AdditionalForecastedCostsExp
   ,''                                                           as C_AnticipatedCosttoCompleteCap
   ,''                                                           as C_AnticipatedCosttoCompleteExp
   ,''                                                           as C_ApprovedChangesandTransfersCap
   ,''                                                           as C_ApprovedChangesandTransfersExp
   ,''                                                           as C_ApprovedCommitmentsCap
   ,''                                                           as C_ApprovedCommitmentsExp
   ,''                                                           as C_OriginalBudgetCap
   ,''                                                           as C_OriginalBudgetExp
   ,''                                                           as C_PendingChangesandTransfersCap
   ,''                                                           as C_PendingChangesandTransfersExp
   ,''                                                           as C_PendingCommitmentsCap
   ,''                                                           as C_PendingCommitmentsExp
   ,''                                                           as C_ProjectedTotalCommitmentsCap
   ,''                                                           as C_ProjectedTotalCommitmentsExp
   ,''                                                           as C_RemainingBalancetoInvoiceCap
   ,''                                                           as C_RemainingBalancetoInvoiceExp
   ,''                                                           as C_TotalApprovedBudgetCap
   ,''                                                           as C_TotalApprovedBudgetExp
   ,''                                                           as C_TotalApprovedInvoicesCap
   ,''                                                           as C_TotalApprovedInvoicesExp
   ,''                                                           as C_TotalInvoicesCap
   ,''                                                           as C_TotalInvoicesExp
   ,''                                                           as C_TotalPendingInvoicesCap
   ,''                                                           as C_TotalPendingInvoicesExp
   ,''                                                           as C_TotalProjectedBudgetCap
   ,''                                                           as C_TotalProjectedBudgetExp
   ,''                                                           as C_VarianceofBudgettoCostCap
   ,''                                                           as C_VarianceofBudgettoCostExp
   ,C_ClientAssetValues                                          as C_ClientAssetValues
   ,C_ClientGrouping                                             as C_ClientGrouping
   ,C_ClientWBSCode                                              as C_ClientWBSCode
   ,''                                                           as C_VendorChangeOrderCost
   ,''                                                           as C_VendorOriginalCost
   ,''                                                           as C_CostCodeName
   ,''                                                           as C_Financial_Complete
   ,''                                                           as C_PackageType
from {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_milestone

union all

 SELECT
	cast(batch_id as char(32))                                   as batch_id
   ,cast(batch_timestamp as timestamp)                           as batch_timestamp
   ,cast(is_deleted as boolean)                                  as is_deleted
   ,cast(id as varchar(100))                                     as id
   ,cast(CreatedBy as varchar(200))                              as CreatedBy
   ,cast(CreatedOn as timestamp)                                 as CreatedOn
   ,cast(LastUpdatedBy as varchar(200))                          as LastUpdatedBy
   ,cast(LastUpdatedOn as timestamp)                             as LastUpdatedOn
   ,cast(Name as varchar(500))                                   as Name
   ,cast(Description as varchar(2000))                           as Description
   ,cast(ExternalID as varchar(1950))                            as ExternalID
   ,cast(State as varchar(2000))                                 as State
   ,cast(StartDate as timestamp)                                 as StartDate
   ,cast(DueDate as timestamp)                                   as DueDate
   ,cast(Duration as decimal(19,4))                              as Duration
   ,cast(ActualStartDate as timestamp)                           as ActualStartDate
   ,cast(ActualEndDate as timestamp)                             as ActualEndDate
   ,cast(TrackStatus as varchar(2000))                           as TrackStatus
   ,cast(Conflicts as decimal(19,4))                             as Conflicts
   ,cast(OnCriticalPath as boolean)                              as OnCriticalPath
   ,cast(DurationManuallySet as boolean)                         as DurationManuallySet
   ,cast(TrackStatusManuallySet as boolean)                      as TrackStatusManuallySet
   ,cast(EarliestStartDate as timestamp)                         as EarliestStartDate
   ,cast(LatestStartDate as timestamp)                           as LatestStartDate
   ,cast(EarliestEndDate as timestamp)                           as EarliestEndDate
   ,cast(LatestEndDate as timestamp)                             as LatestEndDate
   ,cast(ExpectedProgress as decimal(38,10))                     as ExpectedProgress
   ,cast(SchedulingType as varchar(2000))                        as SchedulingType
   ,cast(ImportedFrom as varchar(2000))                          as ImportedFrom
   ,cast(Importance as varchar(2000))                            as Importance
   ,cast(Priority as int)                                        as Priority
   ,cast(PercentCompleted as decimal(38,10))                     as PercentCompleted
   ,cast(Manager as varchar(200))                                as Manager
   ,cast(ChargedTypeManuallySet as boolean)                      as ChargedTypeManuallySet
   ,cast(ChildrenCount as int)                                   as ChildrenCount
   ,cast(SuccessorsCount as int)                                 as SuccessorsCount
   ,cast(PredecessorsCount as int)                               as PredecessorsCount
   ,cast(AlluserResourcesCount as int)                           as AlluserResourcesCount
   ,cast(AttachmentsCount as int)                                as AttachmentsCount
   ,cast(PostsCount as int)                                      as PostsCount
   ,cast(NotesCount as int)                                      as NotesCount
   ,cast(Reportable as boolean)                                  as Reportable
   ,cast(ReportableManuallySet as boolean)                       as ReportableManuallySet
   ,cast(Billable as boolean)                                    as Billable
   ,cast(ChildShortcutCount as int)                              as ChildShortcutCount
   ,cast(Project as varchar(200))                                as Project
   ,cast(WorkPolicy as varchar(2000))                            as WorkPolicy
   ,cast(CommitLevel as varchar(2000))                           as CommitLevel
   ,cast(ReportingStartDate as timestamp)                        as ReportingStartDate
   ,cast(SYSID as varchar(40))                                   as SYSID
   ,cast(Work as decimal(19,4))                                  as Work
   ,cast(ActualEffort as decimal(19,4))                          as ActualEffort
   ,cast(RemainingEffort as decimal(19,4))                       as RemainingEffort
   ,cast(WorkManuallySet as boolean)                             as WorkManuallySet
   ,cast(RemainingEffortManuallySet as boolean)                  as RemainingEffortManuallySet
--    ,cast(WorkVariance as decimal(19,4))                          as WorkVariance
,null                                                       as WorkVariance
   ,cast(ActualDuration as decimal(19,4))                        as ActualDuration
   ,cast(StartDateVariance as decimal(19,4))                     as StartDateVariance
   ,cast(ActualCost as decimal(19,4))                            as ActualCost
   ,cast(DueDateVariance as decimal(19,4))                       as DueDateVariance
   ,cast(PlannedBudget as decimal(19,4))                         as PlannedBudget
   ,cast(DurationVariance as decimal(19,4))                      as DurationVariance
   ,cast(ActualCostManuallySet as boolean)                       as ActualCostManuallySet
   ,cast(PlannedBudgetManuallySet as boolean)                    as PlannedBudgetManuallySet
   ,cast(TimeTrackingEffort as decimal(19,4))                    as TimeTrackingEffort
   ,cast(TimeTrackingCost as decimal(19,4))                      as TimeTrackingCost
   ,cast(FixedCost as decimal(19,4))                             as FixedCost
   ,cast(FixedPrice as decimal(19,4))                            as FixedPrice
   ,cast(PercentInvested as decimal(38,10))                      as PercentInvested
   ,cast(CostVariance as decimal(19,4))                          as CostVariance
   ,cast(TimeTrackingBilling as decimal(19,4))                   as TimeTrackingBilling
   ,cast(EarnedValue as decimal(19,4))                           as EarnedValue
   ,cast(PlannedRevenue as decimal(19,4))                        as PlannedRevenue
   ,cast(CPI as decimal(38,10))                                  as CPI
   ,cast(ActualRevenue as decimal(19,4))                         as ActualRevenue
   ,cast(SPI as decimal(38,10))                                  as SPI
   ,cast(PlannedRevenueManuallySet as boolean)                   as PlannedRevenueManuallySet
   ,cast(ActualRevenueManuallySet as boolean)                    as ActualRevenueManuallySet
   ,cast(Profitability as decimal(19,4))                         as Profitability
   ,cast(PercentProfitability as decimal(38,10))                 as PercentProfitability
   ,cast(PlannedExpenses as decimal(19,4))                       as PlannedExpenses
   ,cast(DirectPlannedExpenses as decimal(19,4))                 as DirectPlannedExpenses
   ,cast(ActualExpenses as decimal(19,4))                        as ActualExpenses
   ,cast(DirectActualExpenses as decimal(19,4))                  as DirectActualExpenses
   ,cast(ProjectedExpenses as decimal(19,4))                     as ProjectedExpenses
   ,cast(DirectProjectedExpenses as decimal(19,4))               as DirectProjectedExpenses
   ,cast(PlannedBilledExpenses as decimal(19,4))                 as PlannedBilledExpenses
   ,cast(DirectPlannedBilledExpenses as decimal(19,4))           as DirectPlannedBilledExpenses
   ,cast(ActualBilledExpenses as decimal(19,4))                  as ActualBilledExpenses
   ,cast(DirectActualBilledExpenses as decimal(19,4))            as DirectActualBilledExpenses
   ,cast(ProjectedBilledExpenses as decimal(19,4))               as ProjectedBilledExpenses
   ,cast(DirectProjectedBilledExpenses as decimal(19,4))         as DirectProjectedBilledExpenses
   ,cast(RevenueCurrencyType as varchar(2000))                   as RevenueCurrencyType
   ,cast(CostCurrencyType as varchar(2000))                      as CostCurrencyType
   ,cast(Pending as varchar(2000))                               as Pending
   ,cast(IssuesCount as int)                                     as IssuesCount
   ,cast(LastUpdatedBySystemOn as timestamp)                     as LastUpdatedBySystemOn
   ,cast(AllowReportingOnSubItems as boolean)                    as AllowReportingOnSubItems
   ,cast(CommittedDate as timestamp)                             as CommittedDate
   ,cast(userResourcesCount as int)                              as userResourcesCount
   ,cast(BudgetedHours as decimal(19,4))                         as BudgetedHours
   ,cast(EmailsCount as int)                                     as EmailsCount
   ,cast(CostBalance as decimal(19,4))                           as CostBalance
   ,cast(BudgetStatus as varchar(2000))                          as BudgetStatus
   ,cast(RevenueBalance as decimal(19,4))                        as RevenueBalance
   ,cast(ActualEffortUpdatedFromTimesheets as boolean)           as ActualEffortUpdatedFromTimesheets
   ,cast(ParentProject as varchar(200))                          as ParentProject
   ,cast(SfExternalId as varchar(256))                           as SfExternalId
   ,cast(SfExternalName as varchar(256))                         as SfExternalName
   ,cast(InternalId as varchar(512))                             as InternalId
   ,cast(OrderID as varchar(512))                                as OrderID
   ,cast(SKU as varchar(512))                                    as SKU
   ,cast(BaselineStartDate as timestamp)                         as BaselineStartDate
   ,cast(BaselineStartDateVariance as decimal(19,4))             as BaselineStartDateVariance
   ,cast(BaselineDueDate as timestamp)                           as BaselineDueDate
   ,cast(BaselineDueDateVariance as decimal(19,4))               as BaselineDueDateVariance
   ,cast(BaselineDuration as decimal(19,4))                      as BaselineDuration
   ,cast(BaselineDurationVariance as decimal(19,4))              as BaselineDurationVariance
   ,cast(BaselineWork as decimal(19,4))                          as BaselineWork
   ,cast(BaselineWorkVariance as decimal(19,4))                  as BaselineWorkVariance
   ,cast(BaselineCost as decimal(19,4))                          as BaselineCost
   ,cast(BaselineCostsVariance as decimal(19,4))                 as BaselineCostsVariance
   ,cast(BaselineRevenue as decimal(19,4))                       as BaselineRevenue
   ,cast(BaselineRevenueVariance as decimal(19,4))               as BaselineRevenueVariance
   ,cast(InternalStatus as varchar(512))                         as InternalStatus
   ,cast(Deliverable as boolean)                                 as Deliverable
   ,cast(DeliverableType as varchar(512))                        as DeliverableType
   ,cast(Executable as boolean)                                  as Executable
   ,cast(Parent as varchar(200))                                 as Parent
   ,cast(PlannedAmount as decimal(19,4))                         as PlannedAmount
   ,cast(ChargedAmount as decimal(19,4))                         as ChargedAmount
   ,cast(TCPI as decimal(38,10))                                 as TCPI
   ,cast(TotalEstimatedCost as decimal(19,4))                    as TotalEstimatedCost
   ,cast(Charged as varchar(2000))                               as Charged
   ,cast(ChargedAmountManuallySet as boolean)                    as ChargedAmountManuallySet
   ,cast(RevenueEarnedValue as decimal(19,4))                    as RevenueEarnedValue
   ,cast(RPI as decimal(38,10))                                  as RPI
   ,cast(RTCPI as decimal(38,10))                                as RTCPI
   ,cast(ReportableStartDate as timestamp)                       as ReportableStartDate
   ,cast(EntityType as varchar(200))                             as EntityType
   ,cast(CompletnessDefinition as decimal(19,4))                 as CompletnessDefinition
   ,cast(ReportableEndDate as timestamp)                         as ReportableEndDate
   ,cast(TaskReportingPolicy as varchar(2000))                   as TaskReportingPolicy
   ,cast(TaskReportingPolicyManuallySet as boolean)              as TaskReportingPolicyManuallySet
   ,cast(FloatingTask as boolean)                                as FloatingTask
   ,cast(IndividualReporting as boolean)                         as IndividualReporting
   ,cast(BaselineCreationDate as timestamp)                      as BaselineCreationDate
   ,cast(AggregatedStopwatchesCount as int)                      as AggregatedStopwatchesCount
   ,cast(CalculateCompletenessBasedOnEfforts as boolean)         as CalculateCompletenessBasedOnEfforts
   ,cast(ActiveStopwatch as varchar(2000))                       as ActiveStopwatch
   ,cast(ObjectAlias as varchar(1000))                           as ObjectAlias
   ,cast(StopwatchesCount as int)                                as StopwatchesCount
   ,cast(LikesCount as int)                                      as LikesCount
   ,cast(BudgetedHoursManuallySet as boolean)                    as BudgetedHoursManuallySet
   ,cast(CurrencyEAC as decimal(28,4))                           as CurrencyEAC
   ,cast(PendingTimeTrackingEffort as decimal(19,4))             as PendingTimeTrackingEffort
   ,cast(CurrencyREAC as decimal(19,4))                          as CurrencyREAC
   ,cast(CurrencyETC as decimal(28,4))                           as CurrencyETC
   ,cast(ImageUrl as varchar(256))                               as ImageUrl
   ,cast(CurrencyRETC as decimal(19,4))                          as CurrencyRETC
   ,cast(SetByLeveling as decimal(19,4))                         as SetByLeveling
   ,cast(GeographicalRegion as varchar(2000))                    as GeographicalRegion
   ,cast(ResourceUtilizationCategory as varchar(2000))           as ResourceUtilizationCategory
   ,cast(StateProvince as varchar(2000))                         as StateProvince
   ,cast(ExpenseType as varchar(2000))                           as ExpenseType
   ,cast(BudgetCostOPEX as decimal(19,4))                        as BudgetCostOPEX
   ,cast(BudgetCostCAPEX as decimal(19,4))                       as BudgetCostCAPEX
   ,cast(BudgetCostNLR as decimal(19,4))                         as BudgetCostNLR
   ,cast(BudgetCostLR as decimal(19,4))                          as BudgetCostLR
   ,cast(EntityOwner as varchar(200))                            as EntityOwner
   ,cast(ActualCostOPEX as decimal(19,4))                        as ActualCostOPEX
   ,cast(ActualCostCAPEX as decimal(19,4))                       as ActualCostCAPEX
   ,cast(ActualCostNLR as decimal(19,4))                         as ActualCostNLR
   ,cast(ActualCostNLROpex as decimal(19,4))                     as ActualCostNLROpex
   ,cast(ActualCostNLRCapex as decimal(19,4))                    as ActualCostNLRCapex
   ,cast(ActualCostLR as decimal(19,4))                          as ActualCostLR
   ,cast(ActualRevenueNLR as decimal(19,4))                      as ActualRevenueNLR
   ,cast(PlannedRevenueNLR as decimal(19,4))                     as PlannedRevenueNLR
   ,cast(FinancialStart as int)                                  as FinancialStart
   ,cast(FinancialEnd as int)                                    as FinancialEnd
   ,cast(FinancialOldStartDate as timestamp)                     as FinancialOldStartDate
   ,cast(RemainingForecastCostNLR as decimal(19,4))              as RemainingForecastCostNLR
   ,cast(RemainingForecastRevenueNLR as decimal(19,4))           as RemainingForecastRevenueNLR
   ,cast(CapexActualYTD as decimal(19,4))                        as CapexActualYTD
   ,cast(OpexActualYTD as decimal(19,4))                         as OpexActualYTD
   ,cast(CapexBudgetYTD as decimal(19,4))                        as CapexBudgetYTD
   ,cast(OpexBudgetYTD as decimal(19,4))                         as OpexBudgetYTD
   ,cast(FYForecastCostCapex as decimal(19,4))                   as FYForecastCostCapex
   ,cast(FYForecastCostOpex as decimal(19,4))                    as FYForecastCostOpex
   ,cast(FYForecastCost as decimal(19,4))                        as FYForecastCost
   ,cast(FYForecastatCompletionCost as decimal(19,4))            as FYForecastatCompletionCost
   ,cast(ProjectForecastatCompletionCost as decimal(19,4))       as ProjectForecastatCompletionCost
   ,cast(ActualBillableHours as decimal(19,4))                   as ActualBillableHours
   ,cast(ActualNonBillableHours as decimal(19,4))                as ActualNonBillableHours
   ,cast(ResourceRateDate as timestamp)                          as ResourceRateDate
   ,cast(LastReplanningDate as timestamp)                        as LastReplanningDate
   ,cast(FYBudgetedCost as decimal(19,4))                        as FYBudgetedCost
   ,cast(FYBudgetedCostCapex as decimal(19,4))                   as FYBudgetedCostCapex
   ,cast(FYBudgetedCostOpex as decimal(19,4))                    as FYBudgetedCostOpex
   ,cast(FYBudgetedCostNLR as decimal(19,4))                     as FYBudgetedCostNLR
   ,cast(FYBudgetedCostLR as decimal(19,4))                      as FYBudgetedCostLR
   ,cast(FYExpectedRevenue as decimal(19,4))                     as FYExpectedRevenue
   ,cast(FYForecastRevenue as decimal(19,4))                     as FYForecastRevenue
   ,cast(ExpectedRevenueLR as decimal(19,4))                     as ExpectedRevenueLR
   ,cast(FYExpectedRevenueLR as decimal(19,4))                   as FYExpectedRevenueLR
   ,cast(PlannedProfit as decimal(19,4))                         as PlannedProfit
   ,cast(FYPlannedProfit as decimal(19,4))                       as FYPlannedProfit
   ,cast(Phase as varchar(2000))                                 as Phase
   ,cast(Country as varchar(2000))                               as Country
   ,cast(C_Visibletocustomer as boolean)                         as C_Visibletocustomer
   ,C_TotalApprovedBudgetAllowance                               as C_TotalApprovedBudgetAllowance
-- ,cast(C_ActualAttendees as decimal(38,10))                    as C_ActualAttendees : Commented Out in RED
   ,C_TotalProjectedBudgetAllowance                              as C_TotalProjectedBudgetAllowance
   ,C_AnticipatedCosttoCompleteExternal                          as C_AnticipatedCosttoCompleteExternal
   ,C_TotalProjectedBudgetExternal                               as C_TotalProjectedBudgetExternal
   ,C_VarianceofBudgettoCostAllowance                            as C_VarianceofBudgettoCostAllowance
   ,C_VarianceofBudgettoCostExternal                             as C_VarianceofBudgettoCostExternal
   ,C_AnticipatedCosttoCompleteAllowance                         as C_AnticipatedCosttoCompleteAllowance
   ,C_TotalApprovedBudgetExternal                                as C_TotalApprovedBudgetExternal
   ,cast(C_AJD60 as varchar(2000))                               as C_AJD60
   ,cast(C_AJD59 as varchar(2000))                               as C_AJD59
   ,cast(C_AJD58 as varchar(2000))                               as C_AJD58
   ,cast(C_AJD57 as decimal(38,10))                              as C_AJD57
   ,cast(C_AJD56 as decimal(38,10))                              as C_AJD56
   ,cast(C_AJD55 as decimal(38,10))                              as C_AJD55
   ,cast(C_AJD54 as decimal(38,10))                              as C_AJD54
   ,cast(C_AJD53 as timestamp)                                   as C_AJD53
   ,cast(C_AJD52 as timestamp)                                   as C_AJD52
   ,cast(C_AJD51 as timestamp)                                   as C_AJD51
   ,cast(C_AJD50 as timestamp)                                   as C_AJD50
   ,cast(C_AJD49 as timestamp)                                   as C_AJD49
   ,cast(C_AJD48 as timestamp)                                   as C_AJD48
   ,cast(C_AJD47 as varchar(2000))                               as C_AJD47
   ,cast(C_AJD46 as varchar(2000))                               as C_AJD46
   ,cast(C_AJD45 as varchar(2000))                               as C_AJD45
   ,cast(C_AJD44 as varchar(2000))                               as C_AJD44
   ,cast(C_AJD43 as varchar(2000))                               as C_AJD43
   ,cast(C_AJD42 as varchar(2000))                               as C_AJD42
   ,cast(C_AJD41 as varchar(2000))                               as C_AJD41
   ,cast(C_AJD40 as varchar(2000))                               as C_AJD40
   ,cast(C_AJD39 as varchar(2000))                               as C_AJD39
   ,cast(C_AJD38 as varchar(2000))                               as C_AJD38
   ,cast(C_AJD37 as varchar(2000))                               as C_AJD37
   ,cast(C_AJD36 as varchar(2000))                               as C_AJD36
   ,cast(C_AJD35 as varchar(2000))                               as C_AJD35
   ,cast(C_AJD34 as varchar(2000))                               as C_AJD34
   ,cast(C_AJD33 as varchar(2000))                               as C_AJD33
   ,cast(C_AJD31 as varchar(2000))                               as C_AJD31
   ,cast(C_AJD32 as varchar(2000))                               as C_AJD32
-- ,cast(C_TotalInvoicesAllowanceRollup as decimal(19,4))        as C_TotalInvoicesAllowanceRollup : Commented Out in RED
   ,cast(C_TotalApprovedInvoicesRollup as decimal(19,4))         as C_TotalApprovedInvoicesRollup
   ,cast(C_TaxAmountRollup as decimal(19,4))                     as C_TaxAmountRollup
   ,cast(C_ReimbursableInvoiceAmountRollup as decimal(19,4))     as C_ReimbursableInvoiceAmountRollup
   ,cast(C_BaseInvoiceAmountRollup as decimal(19,4))             as C_BaseInvoiceAmountRollup
   ,cast(C_ApprovedChangesandTransfers as decimal(19,4))         as C_ApprovedChangesandTransfers
   ,cast(C_AdditionalForecastedCosts as decimal(19,4))           as C_AdditionalForecastedCosts
   ,cast(C_ApprovedChangesandTransferRollup as decimal(19,4))    as C_ApprovedChangesandTransferRollup
   ,cast(C_ApprovedCommitmentsAllowance as decimal(19,4))        as C_ApprovedCommitmentsAllowance
   ,cast(C_PendingCommitmentsAllowance as decimal(19,4))         as C_PendingCommitmentsAllowance
   ,cast(null as decimal(19,4))                                  as C_BaseInvoiceAmount
   ,cast(C_PendingChangesandTransfersRollup as decimal(19,4))    as C_PendingChangesandTransfersRollup
-- ,cast(C_ContractedAmount as decimal(19,4))                    as C_ContractedAmount : Commented Out in RED
   ,cast(C_AdditionalForecastedCostsRollup as decimal(19,4))     as C_AdditionalForecastedCostsRollup
   ,cast(C_OriginalBudgetRollup as decimal(19,4))                as C_OriginalBudgetRollup
   ,cast(C_ReportingId as varchar(2000))                         as C_ReportingId
   ,cast(C_PendingCommitmentsRollup as decimal(19,4))            as C_PendingCommitmentsRollup
   ,cast(null as decimal(19,4))                                  as C_ReimbursableInvoiceAmount
   ,cast(C_ApprovedCommitmentsRollup as decimal(19,4))           as C_ApprovedCommitmentsRollup
   ,cast(C_ReportingName as varchar(2000))                       as C_ReportingName
   ,cast(C_NextGenProgram as boolean)                            as C_NextGenProgram
   ,cast(C_RuntimeId as varchar(2000))                           as C_RuntimeId
   ,cast(C_CostType as varchar(2000))                            as C_CostType
   ,cast(C_MilestoneNumber as decimal(38,10))                    as C_MilestoneNumber
   ,cast(C_CostPerArea as decimal(19,4))                         as C_CostPerArea
   ,cast(C_ServiceProductE1Code as char(2000))                   as C_ServiceProductE1Code
   ,cast(C_ServiceProduct as varchar(2000))                      as C_ServiceProduct
   ,cast(C_AJD30 as varchar(2000))                               as C_AJD30
   ,cast(C_AJD29 as varchar(2000))                               as C_AJD29
   ,cast(C_AJD28 as varchar(2000))                               as C_AJD28
   ,cast(C_AJD26 as varchar(2000))                               as C_AJD26
   ,cast(C_AJD21 as char(2000))                                  as C_AJD21
   ,cast(C_AJD27 as varchar(2000))                               as C_AJD27
   ,cast(C_AJD25 as varchar(2000))                               as C_AJD25
   ,cast(C_AJD24 as varchar(2000))                               as C_AJD24
   ,cast(C_AJD23 as varchar(2000))                               as C_AJD23
   ,cast(C_AJD22 as varchar(2000))                               as C_AJD22
   ,cast(C_VarianceofBudgettoCost as decimal(19,4))              as C_VarianceofBudgettoCost
   ,cast(C_ProgramType as varchar(2000))                         as C_ProgramType
   ,cast(C_AJD20 as char(2000))                                  as C_AJD20
   ,cast(C_AJD19 as varchar(2000))                               as C_AJD19
   ,cast(C_AJD18 as char(2000))                                  as C_AJD18
   ,cast(C_AJD16 as varchar(2000))                               as C_AJD16
   ,cast(C_AJD17 as char(2000))                                  as C_AJD17
   ,cast(C_AJD15 as varchar(2000))                               as C_AJD15
   ,cast(C_AJD14 as varchar(2000))                               as C_AJD14
   ,cast(C_AJD13 as varchar(2000))                               as C_AJD13
   ,cast(C_AJD12 as varchar(2000))                               as C_AJD12
   ,cast(C_AJD11 as varchar(2000))                               as C_AJD11
   ,cast(C_AJD09 as varchar(2000))                               as C_AJD09
   ,cast(C_AJD10 as varchar(2000))                               as C_AJD10
   ,cast(C_AJD08 as varchar(2000))                               as C_AJD08
   ,cast(C_AJD07 as varchar(2000))                               as C_AJD07
   ,cast(C_ScopeStatus as varchar(2000))                         as C_ScopeStatus
   ,cast(C_ScheduleStatus as varchar(2000))                      as C_ScheduleStatus
   ,cast(C_AnticipatedCosttoComplete as decimal(19,4))           as C_AnticipatedCosttoComplete
   ,cast(C_TotalProjectedBudget as decimal(19,4))                as C_TotalProjectedBudget
   ,cast(C_RemainingBalancetoInvoice as decimal(19,4))           as C_RemainingBalancetoInvoice
   ,cast(C_AJD06 as varchar(2000))                               as C_AJD06
   ,cast(C_AJD05 as varchar(2000))                               as C_AJD05
   ,cast(C_AJD04 as varchar(2000))                               as C_AJD04
   ,cast(C_AJD03 as varchar(2000))                               as C_AJD03
   ,cast(C_AJD02 as varchar(2000))                               as C_AJD02
   ,cast(C_AJD01 as varchar(2000))                               as C_AJD01
   ,cast(C_RP30 as varchar(2000))                                as C_RP30
   ,cast(C_RP29 as varchar(2000))                                as C_RP29
   ,cast(C_RP28 as varchar(2000))                                as C_RP28
   ,cast(C_RP27 as varchar(2000))                                as C_RP27
   ,cast(C_RP26 as varchar(2000))                                as C_RP26
   ,cast(C_TotalPendingInvoices as decimal(19,4))                as C_TotalPendingInvoices
   ,cast(C_TotalInvoicesAllowance as decimal(19,4))              as C_TotalInvoicesAllowance
   ,cast(C_ProjectedTotalCommitments as decimal(19,4))           as C_ProjectedTotalCommitments
   ,cast(C_RiskRegistryScore as decimal(38,10))                  as C_RiskRegistryScore
   ,cast(C_RiskRegisterStatus as varchar(2000))                  as C_RiskRegisterStatus
   ,cast(C_ProjectProgramforMeetingMinutes as varchar(200))      as C_ProjectProgramforMeetingMinutes
   ,cast(C_RP23 as varchar(2000))                                as C_RP23
   ,cast(C_RP13 as char(2000))                                   as C_RP13
   ,cast(C_RP12 as varchar(2000))                                as C_RP12
   ,cast(C_RP10 as varchar(2000))                                as C_RP10
   ,cast(C_RP09 as varchar(2000))                                as C_RP09
   ,cast(C_OriginalBudgetExternal as decimal(19,4))              as C_OriginalBudgetExternal
   ,cast(C_TotalInvoicesExternal as decimal(19,4))               as C_TotalInvoicesExternal
   ,cast(C_PendingCommitmentsExternal as decimal(19,4))          as C_PendingCommitmentsExternal
   ,cast(C_ApprovedCommitmentsExternal as decimal(19,4))         as C_ApprovedCommitmentsExternal
   ,cast(C_PendingChangesandTransfersExternal as decimal(19,4))  as C_PendingChangesandTransfersExternal
   ,cast(C_ApprovedChangesandTransfersExternal as decimal(19,4)) as C_ApprovedChangesandTransfersExternal
   ,cast(C_TotalInvoices as decimal(19,4))                       as C_TotalInvoices
   ,cast(C_TotalApprovedBudget as decimal(19,4))                 as C_TotalApprovedBudget
   ,cast(C_TotalApprovedInvoices as decimal(19,4))               as C_TotalApprovedInvoices
   ,cast(C_RP08 as varchar(2000))                                as C_RP08
   ,cast(C_AdditionalForecastedCostsExternal as decimal(19,4))   as C_AdditionalForecastedCostsExternal
   ,cast(C_ApprovedCommitments as decimal(19,4))                 as C_ApprovedCommitments
   ,cast(C_OriginalBudgetAllowance as decimal(19,4))             as C_OriginalBudgetAllowance
   ,cast(C_PendingChangesandTransfers as decimal(19,4))          as C_PendingChangesandTransfers
   ,cast(C_OriginalBudget as decimal(19,4))                      as C_OriginalBudget
   ,cast(C_PendingCommitments as decimal(19,4))                  as C_PendingCommitments
   ,cast(C_Vendor as varchar(200))                               as C_Vendor
   ,cast(C_InternalType as varchar(2000))                        as C_InternalType
   ,cast(C_ProjectBuildingSize as decimal(38,10))                as C_ProjectBuildingSize
   ,cast(C_CAPEXP as varchar(2000))                              as C_CAPEXP
   ,cast(C_Customertask as boolean)                              as C_Customertask
   ,''                                                           as dss_file_path -- DNE in Azara Snapshot Table
   ,''                                                           as dss_loaded_at -- DNE in Azara Snapshot Table
   ,''                                                           as C_OOTB
   ,''                                                           as C_ProjectedEndDate
   ,''                                                           as C_CostPerAreaDEPRECATED
   ,C_CostCode                                                   as C_CostCode
   ,''                                                           as C_AdditionalForecastedCostsCap
   ,''                                                           as C_AdditionalForecastedCostsExp
   ,''                                                           as C_AnticipatedCosttoCompleteCap
   ,''                                                           as C_AnticipatedCosttoCompleteExp
   ,''                                                           as C_ApprovedChangesandTransfersCap
   ,''                                                           as C_ApprovedChangesandTransfersExp
   ,''                                                           as C_ApprovedCommitmentsCap
   ,''                                                           as C_ApprovedCommitmentsExp
   ,''                                                           as C_OriginalBudgetCap
   ,''                                                           as C_OriginalBudgetExp
   ,''                                                           as C_PendingChangesandTransfersCap
   ,''                                                           as C_PendingChangesandTransfersExp
   ,''                                                           as C_PendingCommitmentsCap
   ,''                                                           as C_PendingCommitmentsExp
   ,''                                                           as C_ProjectedTotalCommitmentsCap
   ,''                                                           as C_ProjectedTotalCommitmentsExp
   ,''                                                           as C_RemainingBalancetoInvoiceCap
   ,''                                                           as C_RemainingBalancetoInvoiceExp
   ,''                                                           as C_TotalApprovedBudgetCap
   ,''                                                           as C_TotalApprovedBudgetExp
   ,''                                                           as C_TotalApprovedInvoicesCap
   ,''                                                           as C_TotalApprovedInvoicesExp
   ,''                                                           as C_TotalInvoicesCap
   ,''                                                           as C_TotalInvoicesExp
   ,''                                                           as C_TotalPendingInvoicesCap
   ,''                                                           as C_TotalPendingInvoicesExp
   ,''                                                           as C_TotalProjectedBudgetCap
   ,''                                                           as C_TotalProjectedBudgetExp
   ,''                                                           as C_VarianceofBudgettoCostCap
   ,''                                                           as C_VarianceofBudgettoCostExp
   ,C_ClientAssetValues                                          as C_ClientAssetValues
   ,C_ClientGrouping                                             as C_ClientGrouping
   ,C_ClientWBSCode                                              as C_ClientWBSCode
   ,''                                                           as C_VendorChangeOrderCost
   ,''                                                           as C_VendorOriginalCost
   ,''                                                           as C_CostCodeName
   ,''                                                           as C_Financial_Complete
   ,''                                                           as C_PackageType
from {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_generictask

union all

 SELECT
	cast(batch_id as char(32))                                   as batch_id
   ,cast(batch_timestamp as timestamp)                           as batch_timestamp
   ,cast(is_deleted as boolean)                                  as is_deleted
   ,cast(id as varchar(100))                                     as id
   ,cast(CreatedBy as varchar(200))                              as CreatedBy
   ,cast(CreatedOn as timestamp)                                 as CreatedOn
   ,cast(LastUpdatedBy as varchar(200))                          as LastUpdatedBy
   ,cast(LastUpdatedOn as timestamp)                             as LastUpdatedOn
   ,cast(Name as varchar(500))                                   as Name
   ,cast(Description as varchar(2000))                           as Description
   ,cast(ExternalID as varchar(1950))                            as ExternalID
   ,cast(State as varchar(2000))                                 as State
   ,cast(StartDate as timestamp)                                 as StartDate
   ,cast(DueDate as timestamp)                                   as DueDate
   ,cast(Duration as decimal(19,4))                              as Duration
   ,cast(ActualStartDate as timestamp)                           as ActualStartDate
   ,cast(ActualEndDate as timestamp)                             as ActualEndDate
   ,cast(TrackStatus as varchar(2000))                           as TrackStatus
   ,cast(Conflicts as decimal(19,4))                             as Conflicts
   ,cast('false' as boolean)                                     as OnCriticalPath -- NULL in RED
   ,cast(DurationManuallySet as boolean)                         as DurationManuallySet
   ,cast(TrackStatusManuallySet as boolean)                      as TrackStatusManuallySet
   ,cast(EarliestStartDate as timestamp)                         as EarliestStartDate
   ,cast(LatestStartDate as timestamp)                           as LatestStartDate
   ,cast(EarliestEndDate as timestamp)                           as EarliestEndDate
   ,cast(LatestEndDate as timestamp)                             as LatestEndDate
   ,cast(ExpectedProgress as decimal(38,10))                     as ExpectedProgress
   ,cast(SchedulingType as varchar(2000))                        as SchedulingType
   ,cast(ImportedFrom as varchar(2000))                          as ImportedFrom
   ,cast(Importance as varchar(2000))                            as Importance
   ,cast(Priority as int)                                        as Priority
   ,cast(PercentCompleted as decimal(38,10))                     as PercentCompleted
   ,cast(Manager as varchar(200))                                as Manager
   ,cast(ChargedTypeManuallySet as boolean)                      as ChargedTypeManuallySet
   ,cast(ChildrenCount as int)                                   as ChildrenCount
   ,cast(SuccessorsCount as int)                                 as SuccessorsCount
   ,cast(PredecessorsCount as int)                               as PredecessorsCount
   ,cast(AlluserResourcesCount as int)                           as AlluserResourcesCount
   ,cast(AttachmentsCount as int)                                as AttachmentsCount
   ,cast(PostsCount as int)                                      as PostsCount
   ,cast(NotesCount as int)                                      as NotesCount
   ,cast('false' as boolean)                                     as Reportable -- NULL in RED
   ,cast(ReportableManuallySet as boolean)                       as ReportableManuallySet
   ,cast(Billable as boolean)                                    as Billable
   ,''                                                           as ChildShortcutCount -- NULL in RED
   ,cast(Project as varchar(200))                                as Project
   ,''                                                           as WorkPolicy -- NULL in RED
   ,''                                                           as CommitLevel -- NULL in RED
   ,cast(ReportingStartDate as timestamp)                        as ReportingStartDate
   ,cast(SYSID as varchar(40))                                   as SYSID
   ,cast(Work as decimal(19,4))                                  as Work
   ,cast(ActualEffort as decimal(19,4))                          as ActualEffort
   ,cast(RemainingEffort as decimal(19,4))                       as RemainingEffort
   ,cast(WorkManuallySet as boolean)                             as WorkManuallySet
   ,cast(RemainingEffortManuallySet as boolean)                  as RemainingEffortManuallySet
--    ,cast(WorkVariance as decimal(19,4))                          as WorkVariance
,null                                                       as WorkVariance
   ,cast(ActualDuration as decimal(19,4))                        as ActualDuration
   ,cast(StartDateVariance as decimal(19,4))                     as StartDateVariance
   ,cast(ActualCost as decimal(19,4))                            as ActualCost
   ,cast(DueDateVariance as decimal(19,4))                       as DueDateVariance
   ,cast(PlannedBudget as decimal(19,4))                         as PlannedBudget
   ,cast(DurationVariance as decimal(19,4))                      as DurationVariance
   ,cast(ActualCostManuallySet as boolean)                       as ActualCostManuallySet
   ,cast(PlannedBudgetManuallySet as boolean)                    as PlannedBudgetManuallySet
   ,cast(TimeTrackingEffort as decimal(19,4))                    as TimeTrackingEffort
   ,cast(TimeTrackingCost as decimal(19,4))                      as TimeTrackingCost
   ,cast(FixedCost as decimal(19,4))                             as FixedCost
   ,cast(FixedPrice as decimal(19,4))                            as FixedPrice
   ,cast(PercentInvested as decimal(38,10))                      as PercentInvested
   ,cast(CostVariance as decimal(19,4))                          as CostVariance
   ,cast(TimeTrackingBilling as decimal(19,4))                   as TimeTrackingBilling
   ,cast(EarnedValue as decimal(19,4))                           as EarnedValue
   ,cast(PlannedRevenue as decimal(19,4))                        as PlannedRevenue
   ,cast(CPI as decimal(38,10))                                  as CPI
   ,cast(ActualRevenue as decimal(19,4))                         as ActualRevenue
   ,cast(SPI as decimal(38,10))                                  as SPI
   ,cast(PlannedRevenueManuallySet as boolean)                   as PlannedRevenueManuallySet
   ,cast(ActualRevenueManuallySet as boolean)                    as ActualRevenueManuallySet
   ,cast(Profitability as decimal(19,4))                         as Profitability
   ,cast(PercentProfitability as decimal(38,10))                 as PercentProfitability
   ,cast(PlannedExpenses as decimal(19,4))                       as PlannedExpenses
   ,cast(DirectPlannedExpenses as decimal(19,4))                 as DirectPlannedExpenses
   ,cast(ActualExpenses as decimal(19,4))                        as ActualExpenses
   ,cast(DirectActualExpenses as decimal(19,4))                  as DirectActualExpenses
   ,cast(ProjectedExpenses as decimal(19,4))                     as ProjectedExpenses
   ,cast(DirectProjectedExpenses as decimal(19,4))               as DirectProjectedExpenses
   ,cast(PlannedBilledExpenses as decimal(19,4))                 as PlannedBilledExpenses
   ,cast(DirectPlannedBilledExpenses as decimal(19,4))           as DirectPlannedBilledExpenses
   ,cast(ActualBilledExpenses as decimal(19,4))                  as ActualBilledExpenses
   ,cast(DirectActualBilledExpenses as decimal(19,4))            as DirectActualBilledExpenses
   ,cast(ProjectedBilledExpenses as decimal(19,4))               as ProjectedBilledExpenses
   ,cast(DirectProjectedBilledExpenses as decimal(19,4))         as DirectProjectedBilledExpenses
   ,cast(RevenueCurrencyType as varchar(2000))                   as RevenueCurrencyType
   ,cast(CostCurrencyType as varchar(2000))                      as CostCurrencyType
   ,''                                                           as Pending -- NULL in RED
   ,cast(IssuesCount as int)                                     as IssuesCount
   ,cast(LastUpdatedBySystemOn as timestamp)                     as LastUpdatedBySystemOn
   ,cast(AllowReportingOnSubItems as boolean)                    as AllowReportingOnSubItems
   ,cast(CommittedDate as timestamp)                             as CommittedDate
   ,cast(userResourcesCount as int)                              as userResourcesCount
   ,cast(BudgetedHours as decimal(19,4))                         as BudgetedHours
   ,cast(EmailsCount as int)                                     as EmailsCount
   ,cast(CostBalance as decimal(19,4))                           as CostBalance
   ,cast(BudgetStatus as varchar(2000))                          as BudgetStatus
   ,cast(RevenueBalance as decimal(19,4))                        as RevenueBalance
   ,cast(ActualEffortUpdatedFromTimesheets as boolean)           as ActualEffortUpdatedFromTimesheets
   ,''                                                           as ParentProject -- NULL in RED
   ,cast(SfExternalId as varchar(256))                           as SfExternalId
   ,cast(SfExternalName as varchar(256))                         as SfExternalName
   ,cast(InternalId as varchar(512))                             as InternalId
   ,''                                                           as OrderID -- NULL in RED
   ,cast(SKU as varchar(512))                                    as SKU
   ,cast(BaselineStartDate as timestamp)                         as BaselineStartDate
   ,cast(BaselineStartDateVariance as decimal(19,4))             as BaselineStartDateVariance
   ,cast(BaselineDueDate as timestamp)                           as BaselineDueDate
   ,cast(BaselineDueDateVariance as decimal(19,4))               as BaselineDueDateVariance
   ,cast(BaselineDuration as decimal(19,4))                      as BaselineDuration
   ,cast(BaselineDurationVariance as decimal(19,4))              as BaselineDurationVariance
   ,cast(BaselineWork as decimal(19,4))                          as BaselineWork
   ,cast(BaselineWorkVariance as decimal(19,4))                  as BaselineWorkVariance
   ,cast(BaselineCost as decimal(19,4))                          as BaselineCost
   ,cast(BaselineCostsVariance as decimal(19,4))                 as BaselineCostsVariance
   ,cast(BaselineRevenue as decimal(19,4))                       as BaselineRevenue
   ,cast(BaselineRevenueVariance as decimal(19,4))               as BaselineRevenueVariance
   ,cast(InternalStatus as varchar(512))                         as InternalStatus
   ,cast('false' as boolean)                                     as Deliverable -- NULL in RED
   ,''                                                           as DeliverableType -- NULL in RED
   ,cast('false' as boolean)                                     as Executable -- NULL in RED
   ,''                                                           as Parent -- NULL in RED
   ,cast(PlannedAmount as decimal(19,4))                         as PlannedAmount
   ,cast(ChargedAmount as decimal(19,4))                         as ChargedAmount
   ,cast(TCPI as decimal(38,10))                                 as TCPI
   ,cast(TotalEstimatedCost as decimal(19,4))                    as TotalEstimatedCost
   ,cast(Charged as varchar(2000))                               as Charged
   ,cast(ChargedAmountManuallySet as boolean)                    as ChargedAmountManuallySet
   ,cast(RevenueEarnedValue as decimal(19,4))                    as RevenueEarnedValue
   ,cast(RPI as decimal(38,10))                                  as RPI
   ,cast(RTCPI as decimal(38,10))                                as RTCPI
   ,cast(ReportableStartDate as timestamp)                       as ReportableStartDate
   ,cast(EntityType as varchar(200))                             as EntityType
   ,cast(CompletnessDefinition as decimal(19,4))                 as CompletnessDefinition
   ,cast(ReportableEndDate as timestamp)                         as ReportableEndDate
   ,cast(TaskReportingPolicy as varchar(2000))                   as TaskReportingPolicy
   ,cast(TaskReportingPolicyManuallySet as boolean)              as TaskReportingPolicyManuallySet
   ,cast(FloatingTask as boolean)                                as FloatingTask
   ,cast('false' as boolean)                                     as IndividualReporting -- NULL in RED
   ,cast(BaselineCreationDate as timestamp)                      as BaselineCreationDate
   ,''                                                           as AggregatedStopwatchesCount -- NULL in RED
   ,cast(CalculateCompletenessBasedOnEfforts as boolean)         as CalculateCompletenessBasedOnEfforts
   ,''                                                           as ActiveStopwatch -- NULL in RED
   ,''                                                           as ObjectAlias -- NULL in RED
   ,cast(StopwatchesCount as int)                                as StopwatchesCount
   ,cast(LikesCount as int)                                      as LikesCount
   ,cast(BudgetedHoursManuallySet as boolean)                    as BudgetedHoursManuallySet
   ,cast(CurrencyEAC as decimal(28,4))                           as CurrencyEAC
   ,cast(PendingTimeTrackingEffort as decimal(19,4))             as PendingTimeTrackingEffort
   ,cast(CurrencyREAC as decimal(19,4))                          as CurrencyREAC
   ,cast(CurrencyETC as decimal(28,4))                           as CurrencyETC
   ,cast(ImageUrl as varchar(256))                               as ImageUrl
   ,cast(CurrencyRETC as decimal(19,4))                          as CurrencyRETC
   ,cast(SetByLeveling as decimal(19,4))                         as SetByLeveling
   ,cast(GeographicalRegion as varchar(2000))                    as GeographicalRegion
   ,cast(ResourceUtilizationCategory as varchar(2000))           as ResourceUtilizationCategory
   ,cast(StateProvince as varchar(2000))                         as StateProvince
   ,cast(ExpenseType as varchar(2000))                           as ExpenseType
   ,cast(BudgetCostOPEX as decimal(19,4))                        as BudgetCostOPEX
   ,cast(BudgetCostCAPEX as decimal(19,4))                       as BudgetCostCAPEX
   ,cast(BudgetCostNLR as decimal(19,4))                         as BudgetCostNLR
   ,cast(BudgetCostLR as decimal(19,4))                          as BudgetCostLR
   ,cast(EntityOwner as varchar(200))                            as EntityOwner
   ,cast(ActualCostOPEX as decimal(19,4))                        as ActualCostOPEX
   ,cast(ActualCostCAPEX as decimal(19,4))                       as ActualCostCAPEX
   ,cast(ActualCostNLR as decimal(19,4))                         as ActualCostNLR
   ,cast(ActualCostNLROpex as decimal(19,4))                     as ActualCostNLROpex
   ,cast(ActualCostNLRCapex as decimal(19,4))                    as ActualCostNLRCapex
   ,cast(ActualCostLR as decimal(19,4))                          as ActualCostLR
   ,cast(ActualRevenueNLR as decimal(19,4))                      as ActualRevenueNLR
   ,cast(PlannedRevenueNLR as decimal(19,4))                     as PlannedRevenueNLR
   ,cast(FinancialStart as int)                                  as FinancialStart
   ,cast(FinancialEnd as int)                                    as FinancialEnd
   ,cast(FinancialOldStartDate as timestamp)                     as FinancialOldStartDate
   ,cast(RemainingForecastCostNLR as decimal(19,4))              as RemainingForecastCostNLR
   ,cast(RemainingForecastRevenueNLR as decimal(19,4))           as RemainingForecastRevenueNLR
   ,cast(CapexActualYTD as decimal(19,4))                        as CapexActualYTD
   ,cast(OpexActualYTD as decimal(19,4))                         as OpexActualYTD
   ,cast(CapexBudgetYTD as decimal(19,4))                        as CapexBudgetYTD
   ,cast(OpexBudgetYTD as decimal(19,4))                         as OpexBudgetYTD
   ,cast(FYForecastCostCapex as decimal(19,4))                   as FYForecastCostCapex
   ,cast(FYForecastCostOpex as decimal(19,4))                    as FYForecastCostOpex
   ,cast(FYForecastCost as decimal(19,4))                        as FYForecastCost
   ,cast(FYForecastatCompletionCost as decimal(19,4))            as FYForecastatCompletionCost
   ,cast(ProjectForecastatCompletionCost as decimal(19,4))       as ProjectForecastatCompletionCost
   ,cast(ActualBillableHours as decimal(19,4))                   as ActualBillableHours
   ,cast(ActualNonBillableHours as decimal(19,4))                as ActualNonBillableHours
   ,cast(ResourceRateDate as timestamp)                          as ResourceRateDate
   ,cast(LastReplanningDate as timestamp)                        as LastReplanningDate
   ,cast(FYBudgetedCost as decimal(19,4))                        as FYBudgetedCost
   ,cast(FYBudgetedCostCapex as decimal(19,4))                   as FYBudgetedCostCapex
   ,cast(FYBudgetedCostOpex as decimal(19,4))                    as FYBudgetedCostOpex
   ,cast(FYBudgetedCostNLR as decimal(19,4))                     as FYBudgetedCostNLR
   ,cast(FYBudgetedCostLR as decimal(19,4))                      as FYBudgetedCostLR
   ,cast(FYExpectedRevenue as decimal(19,4))                     as FYExpectedRevenue
   ,cast(FYForecastRevenue as decimal(19,4))                     as FYForecastRevenue
   ,cast(ExpectedRevenueLR as decimal(19,4))                     as ExpectedRevenueLR
   ,cast(FYExpectedRevenueLR as decimal(19,4))                   as FYExpectedRevenueLR
   ,cast(PlannedProfit as decimal(19,4))                         as PlannedProfit
   ,cast(FYPlannedProfit as decimal(19,4))                       as FYPlannedProfit
   ,cast(Phase as varchar(2000))                                 as Phase
   ,cast(Country as varchar(2000))                               as Country
   ,cast(C_Visibletocustomer as boolean)                         as C_Visibletocustomer
   ,C_TotalApprovedBudgetAllowance                               as C_TotalApprovedBudgetAllowance
-- ,cast(C_ActualAttendees as decimal(38,10))                    as C_ActualAttendees : Commented Out in RED
   ,C_TotalProjectedBudgetAllowance                              as C_TotalProjectedBudgetAllowance
   ,C_AnticipatedCosttoCompleteExternal                          as C_AnticipatedCosttoCompleteExternal
   ,C_TotalProjectedBudgetExternal                               as C_TotalProjectedBudgetExternal
   ,C_VarianceofBudgettoCostAllowance                            as C_VarianceofBudgettoCostAllowance
   ,C_VarianceofBudgettoCostExternal                             as C_VarianceofBudgettoCostExternal
   ,C_AnticipatedCosttoCompleteAllowance                         as C_AnticipatedCosttoCompleteAllowance
   ,C_TotalApprovedBudgetExternal                                as C_TotalApprovedBudgetExternal
   ,cast(C_AJD60 as varchar(2000))                               as C_AJD60
   ,cast(C_AJD59 as varchar(2000))                               as C_AJD59
   ,cast(C_AJD58 as varchar(2000))                               as C_AJD58
   ,cast(C_AJD57 as decimal(38,10))                              as C_AJD57
   ,cast(C_AJD56 as decimal(38,10))                              as C_AJD56
   ,cast(C_AJD55 as decimal(38,10))                              as C_AJD55
   ,cast(C_AJD54 as decimal(38,10))                              as C_AJD54
   ,cast(C_AJD53 as timestamp)                                   as C_AJD53
   ,cast(C_AJD52 as timestamp)                                   as C_AJD52
   ,cast(C_AJD51 as timestamp)                                   as C_AJD51
   ,cast(C_AJD50 as timestamp)                                   as C_AJD50
   ,cast(C_AJD49 as timestamp)                                   as C_AJD49
   ,cast(C_AJD48 as timestamp)                                   as C_AJD48
   ,cast(C_AJD47 as varchar(2000))                               as C_AJD47
   ,cast(C_AJD46 as varchar(2000))                               as C_AJD46
   ,cast(C_AJD45 as varchar(2000))                               as C_AJD45
   ,cast(C_AJD44 as varchar(2000))                               as C_AJD44
   ,cast(C_AJD43 as varchar(2000))                               as C_AJD43
   ,cast(C_AJD42 as varchar(2000))                               as C_AJD42
   ,cast(C_AJD41 as varchar(2000))                               as C_AJD41
   ,cast(C_AJD40 as varchar(2000))                               as C_AJD40
   ,cast(C_AJD39 as varchar(2000))                               as C_AJD39
   ,cast(C_AJD38 as varchar(2000))                               as C_AJD38
   ,cast(C_AJD37 as varchar(2000))                               as C_AJD37
   ,cast(C_AJD36 as varchar(2000))                               as C_AJD36
   ,cast(C_AJD35 as varchar(2000))                               as C_AJD35
   ,cast(C_AJD34 as varchar(2000))                               as C_AJD34
   ,cast(C_AJD33 as varchar(2000))                               as C_AJD33
   ,cast(C_AJD31 as varchar(2000))                               as C_AJD31
   ,cast(C_AJD32 as varchar(2000))                               as C_AJD32
-- ,cast(C_TotalInvoicesAllowanceRollup as decimal(19,4))        as C_TotalInvoicesAllowanceRollup : Commented Out in RED
   ,cast(C_TotalApprovedInvoicesRollup as decimal(19,4))         as C_TotalApprovedInvoicesRollup
   ,cast(C_TaxAmountRollup as decimal(19,4))                     as C_TaxAmountRollup
   ,cast(C_ReimbursableInvoiceAmountRollup as decimal(19,4))     as C_ReimbursableInvoiceAmountRollup
   ,cast(C_BaseInvoiceAmountRollup as decimal(19,4))             as C_BaseInvoiceAmountRollup
   ,cast(C_ApprovedChangesandTransfers as decimal(19,4))         as C_ApprovedChangesandTransfers
   ,cast(C_AdditionalForecastedCosts as decimal(19,4))           as C_AdditionalForecastedCosts
   ,cast(C_ApprovedChangesandTransferRollup as decimal(19,4))    as C_ApprovedChangesandTransferRollup
   ,cast(C_ApprovedCommitmentsAllowance as decimal(19,4))        as C_ApprovedCommitmentsAllowance
   ,cast(C_PendingCommitmentsAllowance as decimal(19,4))         as C_PendingCommitmentsAllowance
   ,cast(null as decimal(19,4))                                  as C_BaseInvoiceAmount
   ,cast(C_PendingChangesandTransfersRollup as decimal(19,4))    as C_PendingChangesandTransfersRollup
-- ,cast(C_ContractedAmount as decimal(19,4))                    as C_ContractedAmount : Commented Out in RED
   ,cast(C_AdditionalForecastedCostsRollup as decimal(19,4))     as C_AdditionalForecastedCostsRollup
   ,cast(C_OriginalBudgetRollup as decimal(19,4))                as C_OriginalBudgetRollup
   ,cast(C_ReportingId as varchar(2000))                         as C_ReportingId
   ,cast(C_PendingCommitmentsRollup as decimal(19,4))            as C_PendingCommitmentsRollup
   ,cast(null as decimal(19,4))                                  as C_ReimbursableInvoiceAmount
   ,cast(C_ApprovedCommitmentsRollup as decimal(19,4))           as C_ApprovedCommitmentsRollup
   ,cast(C_ReportingName as varchar(2000))                       as C_ReportingName
   ,cast(C_NextGenProgram as boolean)                            as C_NextGenProgram
   ,cast(C_RuntimeId as varchar(2000))                           as C_RuntimeId
   ,cast(C_CostType as varchar(2000))                            as C_CostType
   ,cast(C_MilestoneNumber as decimal(38,10))                    as C_MilestoneNumber
   ,cast(C_CostPerArea as decimal(19,4))                         as C_CostPerArea
   ,cast(C_ServiceProductE1Code as char(2000))                   as C_ServiceProductE1Code
   ,cast(C_ServiceProduct as varchar(2000))                      as C_ServiceProduct
   ,cast(C_AJD30 as varchar(2000))                               as C_AJD30
   ,cast(C_AJD29 as varchar(2000))                               as C_AJD29
   ,cast(C_AJD28 as varchar(2000))                               as C_AJD28
   ,cast(C_AJD26 as varchar(2000))                               as C_AJD26
   ,cast(C_AJD21 as char(2000))                                  as C_AJD21
   ,cast(C_AJD27 as varchar(2000))                               as C_AJD27
   ,cast(C_AJD25 as varchar(2000))                               as C_AJD25
   ,cast(C_AJD24 as varchar(2000))                               as C_AJD24
   ,cast(C_AJD23 as varchar(2000))                               as C_AJD23
   ,cast(C_AJD22 as varchar(2000))                               as C_AJD22
   ,cast(C_VarianceofBudgettoCost as decimal(19,4))              as C_VarianceofBudgettoCost
   ,cast(C_ProgramType as varchar(2000))                         as C_ProgramType
   ,cast(C_AJD20 as char(2000))                                  as C_AJD20
   ,cast(C_AJD19 as varchar(2000))                               as C_AJD19
   ,cast(C_AJD18 as char(2000))                                  as C_AJD18
   ,cast(C_AJD16 as varchar(2000))                               as C_AJD16
   ,cast(C_AJD17 as char(2000))                                  as C_AJD17
   ,cast(C_AJD15 as varchar(2000))                               as C_AJD15
   ,cast(C_AJD14 as varchar(2000))                               as C_AJD14
   ,cast(C_AJD13 as varchar(2000))                               as C_AJD13
   ,cast(C_AJD12 as varchar(2000))                               as C_AJD12
   ,cast(C_AJD11 as varchar(2000))                               as C_AJD11
   ,cast(C_AJD09 as varchar(2000))                               as C_AJD09
   ,cast(C_AJD10 as varchar(2000))                               as C_AJD10
   ,cast(C_AJD08 as varchar(2000))                               as C_AJD08
   ,cast(C_AJD07 as varchar(2000))                               as C_AJD07
   ,cast(C_ScopeStatus as varchar(2000))                         as C_ScopeStatus
   ,cast(C_ScheduleStatus as varchar(2000))                      as C_ScheduleStatus
   ,cast(C_AnticipatedCosttoComplete as decimal(19,4))           as C_AnticipatedCosttoComplete
   ,cast(C_TotalProjectedBudget as decimal(19,4))                as C_TotalProjectedBudget
   ,cast(C_RemainingBalancetoInvoice as decimal(19,4))           as C_RemainingBalancetoInvoice
   ,cast(C_AJD06 as varchar(2000))                               as C_AJD06
   ,cast(C_AJD05 as varchar(2000))                               as C_AJD05
   ,cast(C_AJD04 as varchar(2000))                               as C_AJD04
   ,cast(C_AJD03 as varchar(2000))                               as C_AJD03
   ,cast(C_AJD02 as varchar(2000))                               as C_AJD02
   ,cast(C_AJD01 as varchar(2000))                               as C_AJD01
   ,cast(C_RP30 as varchar(2000))                                as C_RP30
   ,cast(C_RP29 as varchar(2000))                                as C_RP29
   ,cast(C_RP28 as varchar(2000))                                as C_RP28
   ,cast(C_RP27 as varchar(2000))                                as C_RP27
   ,cast(C_RP26 as varchar(2000))                                as C_RP26
   ,cast(C_TotalPendingInvoices as decimal(19,4))                as C_TotalPendingInvoices
   ,cast(C_TotalInvoicesAllowance as decimal(19,4))              as C_TotalInvoicesAllowance
   ,cast(C_ProjectedTotalCommitments as decimal(19,4))           as C_ProjectedTotalCommitments
   ,cast(C_RiskRegistryScore as decimal(38,10))                  as C_RiskRegistryScore
   ,cast(C_RiskRegisterStatus as varchar(2000))                  as C_RiskRegisterStatus
   ,cast(C_ProjectProgramforMeetingMinutes as varchar(200))      as C_ProjectProgramforMeetingMinutes
   ,cast(C_RP23 as varchar(2000))                                as C_RP23
   ,cast(C_RP13 as char(2000))                                   as C_RP13
   ,cast(C_RP12 as varchar(2000))                                as C_RP12
   ,cast(C_RP10 as varchar(2000))                                as C_RP10
   ,cast(C_RP09 as varchar(2000))                                as C_RP09
   ,cast(C_OriginalBudgetExternal as decimal(19,4))              as C_OriginalBudgetExternal
   ,cast(C_TotalInvoicesExternal as decimal(19,4))               as C_TotalInvoicesExternal
   ,cast(C_PendingCommitmentsExternal as decimal(19,4))          as C_PendingCommitmentsExternal
   ,cast(C_ApprovedCommitmentsExternal as decimal(19,4))         as C_ApprovedCommitmentsExternal
   ,cast(C_PendingChangesandTransfersExternal as decimal(19,4))  as C_PendingChangesandTransfersExternal
   ,cast(C_ApprovedChangesandTransfersExternal as decimal(19,4)) as C_ApprovedChangesandTransfersExternal
   ,cast(C_TotalInvoices as decimal(19,4))                       as C_TotalInvoices
   ,cast(C_TotalApprovedBudget as decimal(19,4))                 as C_TotalApprovedBudget
   ,cast(C_TotalApprovedInvoices as decimal(19,4))               as C_TotalApprovedInvoices
   ,cast(C_RP08 as varchar(2000))                                as C_RP08
   ,cast(C_AdditionalForecastedCostsExternal as decimal(19,4))   as C_AdditionalForecastedCostsExternal
   ,cast(C_ApprovedCommitments as decimal(19,4))                 as C_ApprovedCommitments
   ,cast(C_OriginalBudgetAllowance as decimal(19,4))             as C_OriginalBudgetAllowance
   ,cast(C_PendingChangesandTransfers as decimal(19,4))          as C_PendingChangesandTransfers
   ,cast(C_OriginalBudget as decimal(19,4))                      as C_OriginalBudget
   ,cast(C_PendingCommitments as decimal(19,4))                  as C_PendingCommitments
   ,cast(C_Vendor as varchar(200))                               as C_Vendor
   ,cast(C_InternalType as varchar(2000))                        as C_InternalType
   ,cast(C_ProjectBuildingSize as decimal(38,10))                as C_ProjectBuildingSize
   ,cast(C_CAPEXP as varchar(2000))                              as C_CAPEXP
   ,cast(C_Customertask as boolean)                              as C_Customertask
   ,''                                                           as dss_file_path -- DNE in Azara Snapshot Table
   ,''                                                           as dss_loaded_at -- DNE in Azara Snapshot Table
   ,''                                                           as C_OOTB
   ,''                                                           as C_ProjectedEndDate
   ,''                                                           as C_CostPerAreaDEPRECATED
   ,''                                                           as C_CostCode
   ,''                                                           as C_AdditionalForecastedCostsCap
   ,''                                                           as C_AdditionalForecastedCostsExp
   ,''                                                           as C_AnticipatedCosttoCompleteCap
   ,''                                                           as C_AnticipatedCosttoCompleteExp
   ,''                                                           as C_ApprovedChangesandTransfersCap
   ,''                                                           as C_ApprovedChangesandTransfersExp
   ,''                                                           as C_ApprovedCommitmentsCap
   ,''                                                           as C_ApprovedCommitmentsExp
   ,''                                                           as C_OriginalBudgetCap
   ,''                                                           as C_OriginalBudgetExp
   ,''                                                           as C_PendingChangesandTransfersCap
   ,''                                                           as C_PendingChangesandTransfersExp
   ,''                                                           as C_PendingCommitmentsCap
   ,''                                                           as C_PendingCommitmentsExp
   ,''                                                           as C_ProjectedTotalCommitmentsCap
   ,''                                                           as C_ProjectedTotalCommitmentsExp
   ,''                                                           as C_RemainingBalancetoInvoiceCap
   ,''                                                           as C_RemainingBalancetoInvoiceExp
   ,''                                                           as C_TotalApprovedBudgetCap
   ,''                                                           as C_TotalApprovedBudgetExp
   ,''                                                           as C_TotalApprovedInvoicesCap
   ,''                                                           as C_TotalApprovedInvoicesExp
   ,''                                                           as C_TotalInvoicesCap
   ,''                                                           as C_TotalInvoicesExp
   ,''                                                           as C_TotalPendingInvoicesCap
   ,''                                                           as C_TotalPendingInvoicesExp
   ,''                                                           as C_TotalProjectedBudgetCap
   ,''                                                           as C_TotalProjectedBudgetExp
   ,''                                                           as C_VarianceofBudgettoCostCap
   ,''                                                           as C_VarianceofBudgettoCostExp
   ,C_ClientAssetValues                                          as C_ClientAssetValues
   ,C_ClientGrouping                                             as C_ClientGrouping
   ,C_ClientWBSCode                                              as C_ClientWBSCode
   ,''                                                           as C_VendorChangeOrderCost
   ,''                                                           as C_VendorOriginalCost
   ,''                                                           as C_CostCodeName
   ,''                                                           as C_Financial_Complete
   ,''                                                           as C_PackageType
from {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_program
)

SELECT
   batch_id
  ,batch_timestamp
  ,CASE WHEN UPPER(is_deleted) = 'FALSE' THEN '0'
        WHEN UPPER(is_deleted) = 'TRUE'  THEN '1'
   END as is_deleted
  ,id
  ,CreatedBy
  ,CreatedOn
  ,LastUpdatedBy
  ,LastUpdatedOn
  ,Name
  ,Description
  ,ExternalID
  ,State
  ,StartDate
  ,DueDate
  ,Duration
  ,ActualStartDate
  ,ActualEndDate
  ,TrackStatus
  ,Conflicts
  ,CASE WHEN UPPER(OnCriticalPath) = 'FALSE' THEN '0'
        WHEN UPPER(OnCriticalPath) = 'TRUE'  THEN '1'
   END as OnCriticalPath
  ,CASE WHEN UPPER(DurationManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(DurationManuallySet) = 'TRUE'  THEN '1'
   END as DurationManuallySet
  ,CASE WHEN UPPER(TrackStatusManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(TrackStatusManuallySet) = 'TRUE'  THEN '1'
   END as TrackStatusManuallySet
  ,EarliestStartDate
  ,LatestStartDate
  ,EarliestEndDate
  ,LatestEndDate
  ,ExpectedProgress
  ,SchedulingType
  ,ImportedFrom
  ,Importance
  ,Priority
  ,PercentCompleted
  ,Manager
  ,CASE WHEN UPPER(ChargedTypeManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(ChargedTypeManuallySet) = 'TRUE'  THEN '1'
   END as ChargedTypeManuallySet
  ,ChildrenCount
  ,SuccessorsCount
  ,PredecessorsCount
  ,AlluserResourcesCount
  ,AttachmentsCount
  ,PostsCount
  ,NotesCount
  ,CASE WHEN UPPER(Reportable) = 'FALSE' THEN '0'
        WHEN UPPER(Reportable) = 'TRUE'  THEN '1'
   END as Reportable
  ,CASE WHEN UPPER(ReportableManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(ReportableManuallySet) = 'TRUE'  THEN '1'
   END as ReportableManuallySet
  ,CASE WHEN UPPER(Billable) = 'FALSE' THEN '0'
        WHEN UPPER(Billable) = 'TRUE'  THEN '1'
   END as Billable
  ,ChildShortcutCount
  ,Project
  ,WorkPolicy
  ,CommitLevel
  ,ReportingStartDate
  ,SYSID
  ,Work
  ,ActualEffort
  ,RemainingEffort
  ,CASE WHEN UPPER(WorkManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(WorkManuallySet) = 'TRUE'  THEN '1'
   END as WorkManuallySet
  ,CASE WHEN UPPER(RemainingEffortManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(RemainingEffortManuallySet) = 'TRUE'  THEN '1'
   END as RemainingEffortManuallySet
  ,WorkVariance
  ,ActualDuration
  ,StartDateVariance
  ,ActualCost
  ,DueDateVariance
  ,PlannedBudget
  ,DurationVariance
  ,CASE WHEN UPPER(ActualCostManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(ActualCostManuallySet) = 'TRUE'  THEN '1'
   END as ActualCostManuallySet
  ,CASE WHEN UPPER(PlannedBudgetManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(PlannedBudgetManuallySet) = 'TRUE'  THEN '1'
   END as PlannedBudgetManuallySet
  ,TimeTrackingEffort
  ,TimeTrackingCost
  ,FixedCost
  ,FixedPrice
  ,PercentInvested
  ,CostVariance
  ,TimeTrackingBilling
  ,EarnedValue
  ,PlannedRevenue
  ,CPI
  ,ActualRevenue
  ,SPI
  ,CASE WHEN UPPER(PlannedRevenueManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(PlannedRevenueManuallySet) = 'TRUE'  THEN '1'
   END as PlannedRevenueManuallySet
  ,CASE WHEN UPPER(ActualRevenueManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(ActualRevenueManuallySet) = 'TRUE'  THEN '1'
   END as ActualRevenueManuallySet
  ,Profitability
  ,PercentProfitability
  ,PlannedExpenses
  ,DirectPlannedExpenses
  ,ActualExpenses
  ,DirectActualExpenses
  ,ProjectedExpenses
  ,DirectProjectedExpenses
  ,PlannedBilledExpenses
  ,DirectPlannedBilledExpenses
  ,ActualBilledExpenses
  ,DirectActualBilledExpenses
  ,ProjectedBilledExpenses
  ,DirectProjectedBilledExpenses
  ,RevenueCurrencyType
  ,CostCurrencyType
  ,Pending
  ,IssuesCount
  ,LastUpdatedBySystemOn
  ,CASE WHEN UPPER(AllowReportingOnSubItems) = 'FALSE' THEN '0'
        WHEN UPPER(AllowReportingOnSubItems) = 'TRUE'  THEN '1'
   END as AllowReportingOnSubItems
  ,CommittedDate
  ,userResourcesCount
  ,BudgetedHours
  ,EmailsCount
  ,CostBalance
  ,BudgetStatus
  ,RevenueBalance
  ,CASE WHEN UPPER(ActualEffortUpdatedFromTimesheets) = 'FALSE' THEN '0'
        WHEN UPPER(ActualEffortUpdatedFromTimesheets) = 'TRUE'  THEN '1'
   END as ActualEffortUpdatedFromTimesheets
  ,ParentProject
  ,SfExternalId
  ,SfExternalName
  ,InternalId
  ,OrderID
  ,SKU
  ,BaselineStartDate
  ,BaselineStartDateVariance
  ,BaselineDueDate
  ,BaselineDueDateVariance
  ,BaselineDuration
  ,BaselineDurationVariance
  ,BaselineWork
  ,BaselineWorkVariance
  ,BaselineCost
  ,BaselineCostsVariance
  ,BaselineRevenue
  ,BaselineRevenueVariance
  ,InternalStatus
  ,CASE WHEN UPPER(Deliverable) = 'FALSE' THEN '0'
        WHEN UPPER(Deliverable) = 'TRUE'  THEN '1'
   END as Deliverable
  ,DeliverableType
  ,CASE WHEN UPPER(Executable) = 'FALSE' THEN '0'
        WHEN UPPER(Executable) = 'TRUE'  THEN '1'
   END as Executable
  ,Parent
  ,PlannedAmount
  ,ChargedAmount
  ,TCPI
  ,TotalEstimatedCost
  ,Charged
  ,CASE WHEN UPPER(ChargedAmountManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(ChargedAmountManuallySet) = 'TRUE'  THEN '1'
   END as ChargedAmountManuallySet
  ,RevenueEarnedValue
  ,RPI
  ,RTCPI
  ,ReportableStartDate
  ,EntityType
  ,CompletnessDefinition
  ,ReportableEndDate
  ,TaskReportingPolicy
  ,CASE WHEN UPPER(TaskReportingPolicyManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(TaskReportingPolicyManuallySet) = 'TRUE'  THEN '1'
   END as TaskReportingPolicyManuallySet
  ,CASE WHEN UPPER(FloatingTask) = 'FALSE' THEN '0'
        WHEN UPPER(FloatingTask) = 'TRUE'  THEN '1'
   END as FloatingTask
  ,CASE WHEN UPPER(IndividualReporting) = 'FALSE' THEN '0'
        WHEN UPPER(IndividualReporting) = 'TRUE'  THEN '1'
   END as IndividualReporting  
  ,BaselineCreationDate
  ,AggregatedStopwatchesCount
  ,CASE WHEN UPPER(CalculateCompletenessBasedOnEfforts) = 'FALSE' THEN '0'
        WHEN UPPER(CalculateCompletenessBasedOnEfforts) = 'TRUE'  THEN '1'
   END as CalculateCompletenessBasedOnEfforts  
  ,ActiveStopwatch
  ,ObjectAlias
  ,StopwatchesCount
  ,LikesCount
  ,CASE WHEN UPPER(BudgetedHoursManuallySet) = 'FALSE' THEN '0'
        WHEN UPPER(BudgetedHoursManuallySet) = 'TRUE'  THEN '1'
   END as BudgetedHoursManuallySet 
  ,CurrencyEAC
  ,PendingTimeTrackingEffort
  ,CurrencyREAC
  ,CurrencyETC
  ,ImageUrl
  ,CurrencyRETC
  ,SetByLeveling
  ,GeographicalRegion
  ,ResourceUtilizationCategory
  ,StateProvince
  ,ExpenseType
  ,BudgetCostOPEX
  ,BudgetCostCAPEX
  ,BudgetCostNLR
  ,BudgetCostLR
  ,EntityOwner
  ,ActualCostOPEX
  ,ActualCostCAPEX
  ,ActualCostNLR
  ,ActualCostNLROpex
  ,ActualCostNLRCapex
  ,ActualCostLR
  ,ActualRevenueNLR
  ,PlannedRevenueNLR
  ,FinancialStart
  ,FinancialEnd
  ,FinancialOldStartDate
  ,RemainingForecastCostNLR
  ,RemainingForecastRevenueNLR
  ,CapexActualYTD
  ,OpexActualYTD
  ,CapexBudgetYTD
  ,OpexBudgetYTD
  ,FYForecastCostCapex
  ,FYForecastCostOpex
  ,FYForecastCost
  ,FYForecastatCompletionCost
  ,ProjectForecastatCompletionCost
  ,ActualBillableHours
  ,ActualNonBillableHours
  ,ResourceRateDate
  ,LastReplanningDate
  ,FYBudgetedCost
  ,FYBudgetedCostCapex
  ,FYBudgetedCostOpex
  ,FYBudgetedCostNLR
  ,FYBudgetedCostLR
  ,FYExpectedRevenue
  ,FYForecastRevenue
  ,ExpectedRevenueLR
  ,FYExpectedRevenueLR
  ,PlannedProfit
  ,FYPlannedProfit
  ,Phase
  ,Country
  ,CASE WHEN UPPER(C_Visibletocustomer) = 'FALSE' THEN '0'
        WHEN UPPER(C_Visibletocustomer) = 'TRUE'  THEN '1'
   END as C_Visibletocustomer 
  ,C_TotalApprovedBudgetAllowance
  ,C_TotalProjectedBudgetAllowance
  ,C_AnticipatedCosttoCompleteExternal
  ,C_TotalProjectedBudgetExternal
  ,C_VarianceofBudgettoCostAllowance
  ,C_VarianceofBudgettoCostExternal
  ,C_AnticipatedCosttoCompleteAllowance
  ,C_TotalApprovedBudgetExternal
  ,C_AJD60
  ,C_AJD59
  ,C_AJD58
  ,C_AJD57
  ,C_AJD56
  ,C_AJD55
  ,C_AJD54
  ,C_AJD53
  ,C_AJD52
  ,C_AJD51
  ,C_AJD50
  ,C_AJD49
  ,C_AJD48
  ,C_AJD47
  ,C_AJD46
  ,C_AJD45
  ,C_AJD44
  ,C_AJD43
  ,C_AJD42
  ,C_AJD41
  ,C_AJD40
  ,C_AJD39
  ,C_AJD38
  ,C_AJD37
  ,C_AJD36
  ,C_AJD35
  ,C_AJD34
  ,C_AJD33
  ,C_AJD31
  ,C_AJD32
  ,C_TotalApprovedInvoicesRollup
  ,C_TaxAmountRollup
  ,C_ReimbursableInvoiceAmountRollup
  ,C_BaseInvoiceAmountRollup
  ,C_ApprovedChangesandTransfers
  ,C_AdditionalForecastedCosts
  ,C_ApprovedChangesandTransferRollup
  ,C_ApprovedCommitmentsAllowance
  ,C_PendingCommitmentsAllowance
  ,C_BaseInvoiceAmount
  ,C_PendingChangesandTransfersRollup
  ,C_AdditionalForecastedCostsRollup
  ,C_OriginalBudgetRollup
  ,C_ReportingId
  ,C_PendingCommitmentsRollup
  ,C_ReimbursableInvoiceAmount
  ,C_ApprovedCommitmentsRollup
  ,C_ReportingName
  ,CASE WHEN UPPER(C_NextGenProgram) = 'FALSE' THEN '0'
        WHEN UPPER(C_NextGenProgram) = 'TRUE'  THEN '1'
   END as C_NextGenProgram 
  ,C_RuntimeId
  ,C_CostType
  ,C_MilestoneNumber
  ,C_CostPerArea
  ,C_ServiceProductE1Code
  ,C_ServiceProduct
  ,C_AJD30
  ,C_AJD29
  ,C_AJD28
  ,C_AJD26
  ,C_AJD21
  ,C_AJD27
  ,C_AJD25
  ,C_AJD24
  ,C_AJD23
  ,C_AJD22
  ,C_VarianceofBudgettoCost
  ,C_ProgramType
  ,C_AJD20
  ,C_AJD19
  ,C_AJD18
  ,C_AJD16
  ,C_AJD17
  ,C_AJD15
  ,C_AJD14
  ,C_AJD13
  ,C_AJD12
  ,C_AJD11
  ,C_AJD09
  ,C_AJD10
  ,C_AJD08
  ,C_AJD07
  ,C_ScopeStatus
  ,C_ScheduleStatus
  ,C_AnticipatedCosttoComplete
  ,C_TotalProjectedBudget
  ,C_RemainingBalancetoInvoice
  ,C_AJD06
  ,C_AJD05
  ,C_AJD04
  ,C_AJD03
  ,C_AJD02
  ,C_AJD01
  ,C_RP30
  ,C_RP29
  ,C_RP28
  ,C_RP27
  ,C_RP26
  ,C_TotalPendingInvoices
  ,C_TotalInvoicesAllowance
  ,C_ProjectedTotalCommitments
  ,C_RiskRegistryScore
  ,C_RiskRegisterStatus
  ,C_ProjectProgramforMeetingMinutes
  ,C_RP23
  ,C_RP13
  ,C_RP12
  ,C_RP10
  ,C_RP09
  ,C_OriginalBudgetExternal
  ,C_TotalInvoicesExternal
  ,C_PendingCommitmentsExternal
  ,C_ApprovedCommitmentsExternal
  ,C_PendingChangesandTransfersExternal
  ,C_ApprovedChangesandTransfersExternal
  ,C_TotalInvoices
  ,C_TotalApprovedBudget
  ,C_TotalApprovedInvoices
  ,C_RP08
  ,C_AdditionalForecastedCostsExternal
  ,C_ApprovedCommitments
  ,C_OriginalBudgetAllowance
  ,C_PendingChangesandTransfers
  ,C_OriginalBudget
  ,C_PendingCommitments
  ,C_Vendor
  ,C_InternalType
  ,C_ProjectBuildingSize
  ,C_CAPEXP
  ,CASE WHEN UPPER(C_Customertask) = 'FALSE' THEN '0'
        WHEN UPPER(C_Customertask) = 'TRUE'  THEN '1'
   END as C_Customertask 
  ,dss_file_path
  ,dss_loaded_at
  ,C_OOTB
  ,C_ProjectedEndDate
  ,C_CostPerAreaDEPRECATED
  ,C_CostCode
  ,C_AdditionalForecastedCostsCap
  ,C_AdditionalForecastedCostsExp
  ,C_AnticipatedCosttoCompleteCap
  ,C_AnticipatedCosttoCompleteExp
  ,C_ApprovedChangesandTransfersCap
  ,C_ApprovedChangesandTransfersExp
  ,C_ApprovedCommitmentsCap
  ,C_ApprovedCommitmentsExp
  ,C_OriginalBudgetCap
  ,C_OriginalBudgetExp
  ,C_PendingChangesandTransfersCap
  ,C_PendingChangesandTransfersExp
  ,C_PendingCommitmentsCap
  ,C_PendingCommitmentsExp
  ,C_ProjectedTotalCommitmentsCap
  ,C_ProjectedTotalCommitmentsExp
  ,C_RemainingBalancetoInvoiceCap
  ,C_RemainingBalancetoInvoiceExp
  ,C_TotalApprovedBudgetCap
  ,C_TotalApprovedBudgetExp
  ,C_TotalApprovedInvoicesCap
  ,C_TotalApprovedInvoicesExp
  ,C_TotalInvoicesCap
  ,C_TotalInvoicesExp
  ,C_TotalPendingInvoicesCap
  ,C_TotalPendingInvoicesExp
  ,C_TotalProjectedBudgetCap
  ,C_TotalProjectedBudgetExp
  ,C_VarianceofBudgettoCostCap
  ,C_VarianceofBudgettoCostExp
  ,C_ClientAssetValues
  ,C_ClientGrouping
  ,C_ClientWBSCode
  ,C_VendorChangeOrderCost
  ,C_VendorOriginalCost
  ,C_CostCodeName
  ,C_Financial_Complete
  ,C_PackageType
FROM _workitem

""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_client_mapping: insert
spark.sql(""" 
CREATE OR REPLACE TABLE {var_client_custom_db}.datavault_dbo_r_client_mapping as
  SELECT 
       p.SYSID                             as sys_id    -- (SYSID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
      ,p.Project                           as project   -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
      ,c.ExternalID                        as client_id -- (Client_ID used in hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
      ,p.id                                as id
      ,p.C_ProjectID                       as c_project_id
      ,p.C_JobSiteAddress                  as c_job_site_address
      ,p.name                              as work_item_name
      ,'Project'                           as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP) as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP) as dss_create_at
FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project p
JOIN (SELECT DISTINCT ID, ExternalID FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_customer) c ON p.C_Customer = c.ID
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name = var_client_name, refresh_date=refresh_date))



# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_client_mapping
spark.sql(""" 

with _unionedtables AS (
   SELECT DISTINCT
       p.client_id                           as client_id
      ,gt.id                                 as id           
      ,p.c_project_id                        as c_project_id
      ,''                                    as c_job_site_address
      ,gt.name                               as work_item_name
      ,'TASK'                                as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,r.client_id                           as r_client_id
      ,r.id                                  as r_id
      ,gt.project                            as project
      ,gt.sysid                              as sysid
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_generictask gt
     JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on gt.Project = p.id
LEFT JOIN {var_client_custom_db}.datavault_dbo_r_client_mapping r on gt.id = r.id

union all

   SELECT DISTINCT
       p.client_id                           as client_id
      ,m.id                                  as id           
      ,p.c_project_id                        as c_project_id
      ,''                                    as c_job_site_address
      ,m.name                                as work_item_name
      ,'MILESTONE'                           as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,r.client_id                           as r_client_id
      ,r.id                                  as r_id
      ,m.project                             as project
      ,m.sysid                               as sysid	  
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_milestone m
     JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on m.Project = p.id
LEFT JOIN {var_client_custom_db}.datavault_dbo_r_client_mapping r on m.id = r.id

union all

   SELECT DISTINCT
       p.client_id                           as client_id
      ,pr.id                                 as id           
      ,p.c_project_id                        as c_project_id
      ,''                                    as c_job_site_address
      ,pr.name                               as work_item_name
      ,'PROGRAM'                             as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,r.client_id                           as r_client_id
      ,r.id                                  as r_id
      ,pr.project                            as project
      ,pr.sysid                              as sysid		  
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_program pr
     JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on pr.Project = p.id
LEFT JOIN {var_client_custom_db}.datavault_dbo_r_client_mapping r on pr.id = r.id

union all

   SELECT DISTINCT
       substring(pj.C_Customer,charindex('/',pj.C_Customer,2)+1,50) as client_id 
      ,pj.id                                 as id           
      ,pj.C_ProjectID                        as c_project_id
      ,pj.C_JobSiteAddress                   as c_job_site_address
      ,pj.name                               as work_item_name
      ,'Project'                             as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,p.client_id                           as r_client_id
      ,p.id                                  as r_id
      ,pj.project                            as project
      ,pj.sysid                              as sysid	
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project pj
LEFT JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on pj.id = p.id
)

       MERGE INTO {var_client_custom_db}.datavault_dbo_r_client_mapping r
            USING _unionedtables ut
               ON r.id = ut.r_id
WHEN MATCHED THEN 
       UPDATE SET r.sys_id             = ut.sysid
                 ,r.project            = ut.project       
                 ,r.client_id          = ut.client_id
                 ,r.id                 = ut.id
                 ,r.c_project_id       = ut.c_project_id
                 ,r.c_job_site_address = ut.c_job_site_address
                 ,r.work_item_name     = ut.work_item_name
                 ,r.dss_record_source  = ut.dss_record_source
                 ,r.dss_load_at        = ut.dss_load_at
                 ,r.dss_create_at      = ut.dss_create_at
WHEN NOT MATCHED AND IF(ISNULL(ut.client_id),'ABC',ut.client_id) <> IF(ISNULL(ut.r_client_id),'ABC',ut.r_client_id)
THEN INSERT (sys_id, project, client_id, id, c_project_id, c_job_site_address, work_item_name, dss_record_source, dss_load_at, dss_create_at) 
     VALUES (ut.sysid, ut.project, ut.client_id, ut.id, ut.c_project_id, ut.c_job_site_address, ut.work_item_name, ut.dss_record_source, ut.dss_load_at, ut.dss_create_at)

""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))



# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_users
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_users AS
SELECT
    -- UPPER(MD5(LTRIM(RTRIM(LOWER(
    --     cast(id as string)
    -- ))))) as users_hash_key,
    -- id,
    id as client_id, -- (hash_key_column replacement)
    ExternalID as external_id,
    InternalId as internal_id,
    UserName as user_name,
    FirstName as first_name,
    LastName as last_name,
    Signature as signature,
    JobTitle as job_title,
    OfficePhone as office_phone,
    MobilePhone as mobile_phone,
    C_WorkZip as clz_work_zip,
    C_WorkState as clz_work_state,
    (Case C_Sendignoreinviteemail 
        WHEN True then 1
        WHEN False then 0
        ELSE C_Sendignoreinviteemail
    END) as clz_send_ignore_invite_email,
    C_WorkCountry as clz_work_country,
    C_EmployeeIDPS as clz_employee_id_ps,
    C_BusinessUnitHRPS as clz_business_unit_hrps,
    C_WorkCity as clz_work_city,
    C_WorkAddress as clz_work_address,
    State as state,
    Email as email,
    Profile as profile,
    DisplayName as display_name,
    OfficeFax as office_fax,
    BusinessAddress as business_address,
    HomeAddress as home_address,
    Region as region,
    TimeZone as time_zone,
    NLRCategory as nlr_category,
    AttachmentsCount as attachments_count,
    cast(CurrencyExchangeDate as timestamp) as currency_exchange_at,
    CurrencyExchangeDateManuallySet as currency_exchange_date_manually_set,
    Phase as phase,
    Availability as availability,
    EntityType as entity_type,
    (Case  Admin 
        WHEN True then 1
        WHEN False then 0
        ELSE Admin
    END) as admin,
    (Case ExternalUser 
        WHEN True then 1
        WHEN False then 0
        ELSE ExternalUser
    END) as external_user,
    DirectManager as direct_manager,
    (Case AllowEmails 
        WHEN True then 1
        WHEN False then 0
        ELSE AllowEmails
    END) as allow_emails,
    (Case SendInvitationMail 
        WHEN True then 1
        WHEN False then 0
        ELSE SendInvitationMail
    END) as send_invitation_mail,
    Position as position,
    (Case SubscribeToProjectNotifications 
        WHEN True then 1
        WHEN False then 0
        ELSE SubscribeToProjectNotifications
    END) as subscribe_to_project_notifications,
    Language as language,
    (Case SuperUser 
        WHEN True then 1
        WHEN False then 0
        ELSE SuperUser
    END) as super_user,
    (Case Financial 
        WHEN True then 1
        WHEN False then 0
        ELSE Financial
    END) as financial,
    Location as location,
    TwitterUserName as twitter_user_name,
    TwitterLastMessageId as twitter_last_message_id,
    GoogleUserName as google_user_name,
    LicenseType as license_type,
    CostRegularRate as cost_regular_rate,
    CostOvertimeRate as cost_over_time_rate,
    RevenueRegularRate as revenue_regular_rate,
    RevenueOvertimeRate as revenue_over_time_rate,
    (Case Trial 
        WHEN True then 1
        WHEN False then 0
        ELSE Trial
    END) as trial,
    cast(ExpirationDate as timestamp) as expiration_at,
    ShortDateFormat as short_date_format,
    LastLoginIPAddress as last_login_ip_address,
    StopwatchesCount as stopwatches_count,
    EmailNotifications as email_notifications,
    ImageUrl as image_url,
    NumericFormat as numeric_format,
    MobileProfile as mobile_profile,
    SharepointOnlineUserName as sharepoint_online_user_name,
    TargetUtilization as target_utilization,
    UserSyncId as user_sync_id,
    UserSyncNotes as user_sync_notes,
    C_ReferencedCustomer as clz_referenced_customer,
    cast(UserSyncUpdated as timestamp) as user_sync_updated,
    NotesCount as notes_count,
    cast(LastLogin as timestamp) as last_login_at,
    (Case C_CustomerCollaborator 
        WHEN True then 1
        WHEN False then 0
        ELSE C_CustomerCollaborator
    END) as clz_customer_collaborator,
    C_CCContactReference as clz_cc_contact_reference,
    C_DiscussionGroupURL as clz_discussion_group_url,
    CreatedBy as created_by,
    cast(CreatedOn as timestamp) as created_on_at,
    LastUpdatedBy as last_updated_by,
    cast(LastUpdatedOn as timestamp) as last_updated_on_at,
    'USERS' as dss_record_source,
    CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
    CAST('{refresh_date}' as TIMESTAMP) as dss_start_at,
    CAST('{refresh_date}' as TIMESTAMP) as dss_create_at,
    -- dss_version  (no history maintained)
    C_BusinessLine as clz_business_line,
    C_Function as clz_function,
    C_MajorMarket as clz_major_market,
    C_MarketDescription as clz_market_description,
    '' as clz_pspl,
    C_SecGrouping as clz_sec_grouping,
    C_StrategicClient as clz_strategic_client,
    C_UserActivityCount as clz_user_activity_count,
    C_WorkerType as clz_worker_type,
    TimesheetsCompleteUntil as time_sheets_complete_until,
    TimesheetsExpectedFrom as time_sheets_expected_from,
    C_TeamContact as clz_team_contact,
    C_PrimaryCustomer as clz_primary_customer,
    ScimSyncId as scim_sync_id,
    TimesheetApprovalDelegatesCount as timesheet_approval_delegates_count,
    TimesheetApprovalDelegatorsCount as timesheet_approval_delegators_count
    -- dss_current_version (no history maintained)
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_user
where
    is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))



# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_risk_details
spark.sql("""  
CREATE OR REPLACE TABLE {var_client_custom_db}.datavault_dbo_s_risk_details
AS
SELECT
    cm.client_id as client_id,
    SYSID as sys_id,
    ExternalID as external_id,
    PercentCompleted as percent_completed,
    AttachmentsCount as attachments_count,
    PostsCount as posts_count,
    NotesCount as notes_count,
    Title as title,
    Severity as severity,
    Priority as priority,
    (case Mandatory 
        WHEN True then 1
        WHEN False then 0
        else Mandatory
    END) as mandatory,
    Owner as owner,
    DueDate as due_at,
    AssignedTo as assigned_to,
    AssignmentDate as assignment_at,
    SubmittedBy as submitted_by,
    SubmissionDate as submission_at,
    PlannedFor as planned_for,
    Description as description,
    PlainText as plaintext,
    BusinessImpact as business_impact,
    Score as score,
    State as state,
    EntityType as entity_type,
    EntityOwner as entity_owner,
    PercentProbability as percent_probability,
    Impact as impact,
    TriggerDate as trigger_at,
    MitigationPlan as mitigation_plan,
    RiskState as risk_state,
    OriginatedFrom as originated_from,
    (case C_WFTrigger 
        WHEN True then 1
        WHEN False then 0
        else C_WFTrigger
    END) as clz_wf_trigger,
    C_Impact as clz_impact,
    (case C_JLLInternalRisk 
        WHEN True then 1
        WHEN False then 0
        else C_JLLInternalRisk
    END)as clz_jll_internal_risk,
    C_EstimatedRiskFactorScore as clz_estimated_risk_factor_score,
    C_InitialRiskFactorScore as clz_initial_risk_factor_score,
    C_InitialLikelihoodScore as clz_initial_likelihood_score,
    C_EstimatedLikelihoodScore as clz_estimated_likelihood_score,
    C_EstimatedConsequencesScore as clz_estimated_consequences_score,
    C_InitialConsequencesScore as clz_initial_consequences_score,
    C_RiskStatus as clz_risk_status,
    C_EstimatedConsequences as clz_estimated_consequences,
    C_EstimatedLikelihood as clz_estimated_likelihood,
    C_ActiontoAchieveMitigation as clz_action_to_achieve_mitigation,
    C_MitigationType as clz_mitigation_type,
    C_RiskBearer as clz_risk_bearer,
    C_InitialConsequences as clz_initial_consequences,
    C_InitialLikelihood as clz_initial_likelihood,
    C_ImpactofOccurrence as clz_impact_of_occurrence,
    C_RiskCategory as clz_risk_category,
    C_RiskRating as clz_risk_rating,
    C_Likelihood as clz_likelihood,
    CreatedBy as created_by,
    replace(date_format(CreatedOn,'yyyy-MM-dd HH:mm:ss.SSSSSS'),'T',' ') as created_at,
    LastUpdatedBy as last_updated_by,
    LastUpdatedOn as last_updated_at,
    EvaluatedBy as evaluated_by,
    EvaluationDate as evaluationd_at,
    OpeningDate as opening_at,
    ResolvedBy as resolved_by,
    ResolutionDetails as resolution_details,
    ResolvedDate as resolved_at,
    ClosureDate as closure_at,
    'RISK' as dss_record_source,
    current_timestamp() as dss_load_at,
    current_timestamp() as dss_start_at,
    current_timestamp() as dss_create_at,
    -- as dss_version, (no history maintained)
    C_CostProbability as clz_cost_probability,
    C_TotalCostifOccurs as clz_total_cost_if_occurs,
    C_WeightedCost as clz_weighted_cost,
    C_RiskType as clz_risk_type,
    Comment as comment,
    C_RiskReview as clz_risk_review,
    C_ResponsibleParty as clz_responsible_party,
    -- as dss_current_version,  (no history maintained)
    (case C_Copied 
        WHEN True then 1
        WHEN False then 0
        else C_Copied
    END) as C_Copied,
    C_Meeting as C_Meeting,
    (case C_BAURisk 
        WHEN True then 1
        WHEN False then 0
        else C_BAURisk
    END) as C_BAURisk
FROM
    {var_azara_raw_db}.edp_snapshot_clarizen_risk r
left join 
    (select 
        distinct 
        id, 
        client_id 
    from 
        {var_client_custom_db}.datavault_dbo_r_client_mapping
    ) cm
on 
    r.PlannedFor = cm.id 
where
    is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db))



# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_related_work_link
spark.sql("""  
CREATE OR REPLACE TABLE {var_client_custom_db}.datavault_dbo_s_related_work_link
AS
SELECT
	w.client_id  as client_id, --(hash Key replacement)
	w.sys_id as sys_id, --(hash Key replacement use if required it is sourced from r_client_mapping)
	w.project as project, --(hash Key replacement use if required it is sourced from r_client_mapping)
	createdby as created_by,
	createdon as created_at,
	LastUpdatedBy as last_updated_by,
	lastupdatedby as workitem,
	case as c_case,
	ExternalID as external_id,
	ImpactWeight as impact_weight,
	EntityType as entity_type,
	'RelatedWork' as dss_record_source,
	current_timestamp() as dss_load_at,
	current_timestamp() as dss_start_at,
	current_timestamp() as dss_create_at,
	r.id as id --(hash Key replacement)
FROM
    {var_azara_raw_db}.edp_snapshot_clarizen_relatedwork r
left join 
    (select 
    	distinct 
     	id, 
     	client_id,
		sys_id,
		project
    from 
    	{var_client_custom_db}.datavault_dbo_r_client_mapping
    ) w   
on 
    w.id = r.workitem
where
	is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_clients
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_r_clients AS
select
	C_CustomerID as client_id,
	coalesce(C.C_OVClientID, RC.ovclientid) as company_id,
	coalesce(C.Name, RC.clientname) as client_name,
	ExternalID as client_code,
	SYSID as source_system_id,
	C_PeoplesoftCustomerID as peoplesoft_id,
	'stage_clarizen.dbo.customer' as dss_record_source,
	CAST('{refresh_date}' as TIMESTAMP) as dss_create_time,
	CAST('{refresh_date}' as TIMESTAMP) as dss_update_time,
	'' as go_live_date,
	'' as network_id
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_customer C 
left join 
    {var_client_custom_db}.{var_client_name}_ovp_tbl_raw_all_userdata_clients RC
    on RC.clientid = C.C_CustomerID 
where
    C.is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_program_details
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_program_details AS
SELECT
    -- UPPER(MD5(concat(
    --     LTRIM(RTRIM(LOWER(cast(id as string)))),
    --     LTRIM(RTRIM(LOWER(cast(SUBSTRING_INDEX(C_ProgramCustomer, '/', -1) as string))))
    -- ))) as program_hash_key, ( no hash key required)
    --  as program_details_hash_diff, (no history maintained)
    id, -- (id added in place of haskey)
    SUBSTRING_INDEX(C_ProgramCustomer, '/', -1) as client_id,
    ExternalID as external_id,
    State as state,
    SYSID as sys_id,
    (Case C_Customertask 
        WHEN True then 1
        WHEN False then 0
        ELSE C_Customertask
    END) as clz_customer_task,
    EntityType as entity_type,
    Name as name,
    TrackStatus as track_status,
    BudgetStatus as budget_status,
    (Case TrackStatusManuallySet 
        WHEN True then 1
        WHEN False then 0
        ELSE TrackStatusManuallySet
    END) as track_status_manually_set,
    LikesCount as likes_count,
    ExpectedProgress as expected_progress,
    SchedulingType as scheduling_type,
    InternalStatus as internal_status,
    Priority as priority,
    Manager as manager,
    PostsCount as posts_count,
    ProjectManager as project_manager,
    Phase as phase,
    Program as program,
    ProgramManager as program_manager,
    Justification as justification,
    LastUpdatedBy as last_updated_by,
    BusinessAlignment as business_alignment,
    HoldingNotes as holding_notes,
    PlannedSavings as planned_savings,
    ClosingNotes as closing_notes,
    ExpectedBusinessValue as expected_business_value,
    cast(LastUpdatedOn as TIMESTAMP) as last_updated_at,
    ProgramGoals as program_goals,
    OverallSummary as overall_summary,
    ScheduleSummary as schedule_summary,
    AdditionalComments as additional_comments,
    cast(CreatedOn as TIMESTAMP) as created_at,
    C_ProgramCustomer as clz_program_customer,
    C_ProgramDocumentFolder as clz_program_document_folder,
    C_ProgramRegion as clz_program_region,
    (Case FloatingTask 
        WHEN True then 1
        WHEN False then 0
        ELSE FloatingTask
    END) as floating_task,
    CreatedBy as created_by,
    Description as description,
    BudgetSummary as budget_summary,
    PendingTimeTrackingEffort as pending_time_tracking_effort,
    C_ServiceProductE1Code as clz_service_product_e1_code,
    C_ProgramType as clz_program_type,
    C_ScopeStatus as clz_scope_status,
    C_MilestoneNumber as clz_milestone_number,
    C_RiskRegistryScore as clz_risk_registry_score,
    Risks as risks,
    RisksRate as risks_rate,
    RisksImpact as risks_impact,
    RisksTotalScore as risks_total_score,
    C_RiskRegisterStatus as clz_risk_register_status,
    C_ReportingName as clz_reporting_name,
    C_ReportingId as clz_reporting_id,
    C_ScheduleStatus as clz_schedule_status,
    C_Vendor as clz_vendor,
    C_RuntimeId as clz_runtime_id,
    C_InternalType as clz_internal_type,
    C_ProjectBuildingSize as clz_project_building_size,
    C_ServiceProduct as clz_service_product,
    (Case C_NextGenProgram 
        WHEN True then 1
        WHEN False then 0
        ELSE C_NextGenProgram
    END) as clz_next_gen_program,
    C_ProjectProgramforMeetingMinutes as clz_project_program_for_meeting_minutes,
    (Case C_Visibletocustomer 
        WHEN True then 1
        WHEN False then 0
        ELSE C_Visibletocustomer
    END) as clz_visible_to_customer,
    C_AJD30 as clz_ajd30,
    C_AJD23 as clz_ajd23,
    C_AJD25 as clz_ajd25,
    C_AJD28 as clz_ajd28,
    C_AJD07 as clz_ajd07,
    C_AJD19 as clz_ajd19,
    C_AJD20 as clz_ajd20,
    C_AJD29 as clz_ajd29,
    C_AJD27 as clz_ajd27,
    C_AJD26 as clz_ajd26,
    C_AJD21 as clz_ajd21,
    C_AJD24 as clz_ajd24,
    C_AJD22 as clz_ajd22,
    C_AJD18 as clz_ajd18,
    C_AJD16 as clz_ajd16,
    C_AJD17 as clz_ajd17,
    C_AJD15 as clz_ajd15,
    C_AJD14 as clz_ajd14,
    C_AJD13 as clz_ajd13,
    C_AJD12 as clz_ajd12,
    C_AJD11 as clz_ajd11,
    C_AJD09 as clz_ajd09,
    C_AJD10 as clz_ajd10,
    C_AJD08 as clz_ajd08,
    C_AJD06 as clz_ajd06,
    C_AJD05 as clz_ajd05,
    C_AJD04 as clz_ajd04,
    C_AJD03 as clz_ajd03,
    C_AJD02 as clz_ajd02,
    C_AJD01 as clz_ajd01,
    C_RP30 as clz_rp30,
    C_RP29 as clz_rp29,
    C_RP28 as clz_rp28,
    C_RP27 as clz_rp27,
    C_RP26 as clz_rp26,
    C_RP23 as clz_rp23,
    C_RP13 as clz_rp13,
    C_RP12 as clz_rp12,
    C_RP10 as clz_rp10,
    C_RP09 as clz_rp09,
    C_RP08 as clz_rp08,
    (Case RollupFinancialAndEffortDataFromShortcut 
        WHEN True then 1
        WHEN False then 0
        ELSE RollupFinancialAndEffortDataFromShortcut
    END) as rollup_financial_and_effort_data_from_shortcut,
    BudgetVariancePercent as budget_variance_percent,
    LastFPCalculationDate as last_fp_calculation_date_at,
    RateCard as rate_card,
    BillingType as billing_type,
    BillingTypeManuallySet as billing_type_manually_set,
    RemainingBudget as remaining_budget,
    BudgetVariance as budget_variance,
    DefaultIntegrationPath as default_integration_path,
    (Case ActualEffortUpdatedFromTimesheets 
        WHEN True then 1
        WHEN False then 0
        ELSE ActualEffortUpdatedFromTimesheets
    END) as actual_effort_updated_from_time_sheets,
    CAST(LastUpdatedBySystemOn as TIMESTAMP) as last_updated_by_system_on_at,
    'PROGRAM' as dss_record_source,
    CAST('{refresh_date}' as TIMESTAMP) as dss_load_date,
    CAST('{refresh_date}' as TIMESTAMP) as dss_start_date,
    CAST('{refresh_date}' as TIMESTAMP) as dss_create_time
    --  as dss_version, (no history maintained)
    --  as dss_current_version, (no history maintained)
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_program
where
    is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from jll_azara_catalog.jll_azara_0007745730_capitalone_custom.capitalone_raw_edp_snapshot_clarizen_program

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_project_address
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.datavault_dbo_s_project_address AS
SELECT
      id --(hash_key_replacement)
    , SUBSTRING_INDEX(C_Customer, '/', -1)  as client_id
    , C_MDMID as c_mdm_id
    , C_PostalCode as c_postal_code
    , StateProvince as state_province
    , C_City as c_city
    , C_Address as c_address
    , GeographicalRegion as geographical_region
    , Country as country
    , C_Longitude as c_longitude
    , C_Latitude as c_latitude
    , C_JobSiteAddress as c_job_site_address
    , C_SpendJobsiteAddressE1Code as c_spend_job_site_address_e1_code
    , C_SpendJobsitePropertyType as c_spend_job_site_property_type
    , 'PROJECT' as dss_record_source
    , CAST('{refresh_date}' as TIMESTAMP) as dss_load_date
    , CAST('{refresh_date}' as TIMESTAMP) as dss_create_time 
    , CAST('{refresh_date}' as TIMESTAMP) as dss_update_time 
    , cast(LastUpdatedOn as timestamp) as last_updated_at
    , LastUpdatedBy as last_updated_by
    , cast(LastUpdatedBySystemOn as timestamp) as last_updated_by_system_on_at
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project
where
    is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_customer
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.datavault_dbo_s_customer AS
SELECT 
    id, --(hash key replacement)
    substring_index(id,'/',-1)             as client_id,                                            
    ExternalID                             as external_id,                                         
    SYSID                                  as sys_id,                                              
    InternalID                             as internal_id,                                         
    SfExternalId                           as sf_external_id,                                      
    C_CustomerID                           as c_customer_id,                                       
    C_BoxFolderID                          as c_box_folder_id,                                     
    C_OVClientID                           as clz_ov_client_id,                                    
    Name                                   as name,                                                
    Owner                                  as owner,                                               
    Description                            as description,                                         
    AccountStatus                          as account_status,                                      
    SfExternalName                         as sf_external_name,                                    
    Tier                                   as tier,                                                
    EntityType                             as entity_type,                                         
    Industry                               as industry,                                            
    LastDateContacted                      as last_date_contacted,                                 
    CustomerValue                          as customer_value,                                      
    URL                                    as url,                                                 
    MarketingSource                        as marketing_source,                                    
    TechnicalAccountManager                as technical_account_manager,                           
    CustomerSuccessStatus                  as customer_success_status,                             
    CompanySize                            as company_size,                                        
    OfficePhone                            as office_phone,                                        
    AttachmentsCount                       as attachments_count,            
    LikesCount                             as likes_count,                                         
    EmailsCount                            as emails_count,                                        
    ImageUrl                               as image_url,                                           
    EntityOwner                            as entity_owner,                                        
    CurrencyExchangeDate                   as currency_exchange_date,                              
    CurrencyExchangeDateManuallySet        as currency_exchange_date_manually_set,                
    NotesCount                             as notes_count,                                         
    C_ClientIndustry                       as c_client_industry,                                   
    C_ClientIndustryE1Code                 as c_client_industry_e1_code,                           
    C_ServiceLine                          as c_service_line,                                      
    (case C_PropertyHubIntegration 
        WHEN True then 1
        WHEN False then 0
        else C_PropertyHubIntegration
    END)                                   as c_property_hub_integration,                          
    C_PeoplesoftCustomerID                 as c_peoplesoft_customer_id,                            
    C_PeoplesoftCustomerStatus             as c_peoplesoft_customer_status,                        
    (case C_SpendIntegration
        WHEN True then 1
        WHEN False then 0
        else C_SpendIntegration
    END)                                   as c_spend_integration,                                 
    C_PrimaryColorCustomer                 as c_primary_color_customer,                            
    C_DiscussionGroupReference             as c_discussion_group_reference,                        
    C_LogoImage                            as c_logo_image,                                        
    CreatedBy                              as created_by,                                          
    CreatedOn                              as created_on,                                          
    LastUpdatedBy                          as last_updated_by,                                     
    LastUpdatedOn                          as last_updated_on,                                     
    'CUSTOMER'                             as dss_record_source,                                   
    CAST('{refresh_date}' as TIMESTAMP)    as dss_load_date,                                       
    CAST('{refresh_date}' as TIMESTAMP)    as dss_start_date,                                      
    CAST('{refresh_date}' as TIMESTAMP)    as dss_create_time,     
    CAST('{refresh_date}' as TIMESTAMP)    as dss_update_time,                                   
    State                                  as state,                                               
    C_MagnitudeofImpact                    as clz_magnitude_of_impact,                             
    (case C_WFToggle
        WHEN True then 1
        WHEN False then 0
        else C_WFToggle
    END)                                   as clz_wf_toggle,                                       
    C_ActiveApplications                   as clz_active_applications,                             
    C_PortalBackground                     as clz_portal_background,                               
    C_SurveyLink                           as clz_survey_link,                                     
    C_TimingofImpact                       as clz_timing_of_impact,                                
    C_OrganizationalImpact                 as clz_organizational_impact,                           
    C_StrategicAlignment                   as clz_strategic_alignment,                             
    C_Payback                              as clz_pay_back,                                        
    C_ProbabilityofImpact                  as clz_probability_of_impact,                           
    C_AdditionalFieldsSource               as clz_additional_fields_source,                        
    C_CleanedCustomerName                  as clz_cleaned_customer_name,                           
    C_CustomerDocumentFolder               as clz_customer_document_folder,                        
    C_CustomerParent                       as clz_customer_parent,                                 
    C_ProjectDriver                        as clz_project_driver,                                  
    DefaultIntegrationPath                 as default_integration_path,                            
    C_AttributeScoresSource                as clz_attribute_scores_source,                         
    C_Classification                       as clz_classification,                                  
    C_ConditionScore                       as clz_condition_score,                                 
    C_CPNewProjectForm                     as clz_cp_new_project_form,                             
    (case C_CustomerFieldToggle
        WHEN True then 1
        WHEN False then 0
        else C_CustomerFieldToggle
    END)                                   as clz_customer_field_toggle,                           
    C_DiscussionGroupReference             as clz_discussion_group_reference,                      
    C_Element                              as clz_element,                                         
    C_Experience                           as clz_experience,                                      
    C_LocationCharacteristic               as clz_location_characteristic,                         
    (case C_RestrictWidgetSharing
        WHEN True then 1
        WHEN False then 0
        else C_RestrictWidgetSharing
    END)                                   as clz_restrict_widget_sharing,                         
    LastUpdatedBySystemOn                  as last_updated_by_system_on_at,                                                  
    (case C_isQuarantineExempted 
        WHEN True then 1
        WHEN False then 0
        else C_isQuarantineExempted
    END)                                    as clz_is_quarintine_exempted,   
    --null                                       as clz_is_quarintine_exempted,                   
    (case C_isUnsubscribeExempted 
        WHEN True then 1
        WHEN False then 0
        else C_isUnsubscribeExempted
    END)                                    as clz_is_unsubscribe_exempted,
    --null                                        as clz_is_unsubscribe_exempted,
    C_MedalliaId              as clz_medallia_id,     
    --null                          as clz_medallia_id,                                                         
    C_SurveyIntegration                     as clz_survey_integration,  
    --null                                        as clz_survey_integration,                           
     C_PSIndustry                            as clz_ps_industry  
    --null                            as clz_ps_industry                              
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_customer

""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_project_details
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_project_details AS
SELECT
    -- UPPER(MD5(LTRIM(RTRIM(LOWER(
    --     cast(id as string)
    -- ))))) as project_hash_key,
    -- as project_hash_diff, (no history maintained)
    id, --(hash key replacement)
    SYSID as sys_id,
    SUBSTRING_INDEX(C_Customer, '/', -1)  as client_id,
    C_ProjectID as c_project_id,
    Project as project,
    Program as program,
    C_ProjectNumber as c_project_number,
    ProjectManager as project_manager,
    C_ProjectCoordinator as c_project_coordinator,
    ProjectType as project_type,
    -- as c_project_status, (data found in red need to map)
    C_ProjectPhotoFile as c_project_photo_file,
    C_ArchitectProjectNumber as c_architect_project_number,
    C_TeamLead as c_team_lead,
    '' as c_agency,
    C_ContractBusinessUnit as clz_contract_business_unit,
    C_PSPL as c_pspl,
    C_PSInvoiceOfficeLocation as c_ps_invoice_office_location,
    C_PSCountry as c_ps_country,
    C_PSHub as c_ps_hub,
    '' as c_anz_financial_year,
    '' as c_anz_project_type,
    '' as c_ryg_status,
    '' as c_budget_template_deprecated,
    '' as c_home_office_location,
    Name as name,
    ParentProject as parent_project,
    Parent as parent,
    C_State as c_state,
    State as state,
    C_Region as c_region,
    OverallSummary as overall_summary,
    BudgetStatus as budget_status,
    C_3rdPartyCertification as c_3rd_party_certification,
    LikesCount as likes_count,
    PostsCount as posts_count,
    Phase as phase,
    C_PhaseE1Code as c_phase_e1_code,
    C_OVCPID as c_ovcp_id,
    C_OvalID as c_oval_id,
    C_SpendLastUpdatedOn as c_spend_last_updated_on,
    CreatedBy as created_by,
    CreatedOn as created_on,
    Description as description,
    LastUpdatedBy as last_updated_by,
    LastUpdatedOn as last_updated_on,
    C_AgreementType as c_agreement_type,
    -- as c_ootb,
    C_ProjectProgramforMeetingMinutes as c_project_program_for_meeting_minutes,
    C_JobTemplate as c_job_template,
    C_ProjectArea as c_project_area,
    C_JLLProjectType as c_jll_project_type,
    C_JLLRoleE1Code as c_jll_role_e1_code,
    C_JLLRole as c_jll_role,
    C_ServiceProductE1Code as c_service_product_e1_code,
    C_ProgramType as c_program_type,
    C_ScopeStatus as c_scope_status,
    C_MilestoneNumber as c_milestone_number,
    C_RiskRegistryScore as clz_risk_registry_score,
    C_RiskRegisterStatus as c_risk_register_status,
    C_ReportingName as c_reporting_name,
    C_ReportingId as c_reporting_id,
    C_ScheduleStatus as c_schedule_status,
    C_Vendor as c_vendor,
    C_RuntimeId as c_runtime_id,
    C_InternalType as c_internal_type,
    C_ProjectBuildingSize as c_project_building_size,
    C_BuildingSize as c_building_size,
    C_APACAssetType as c_apac_asset_type,
    C_APACProjectStage as c_apac_project_stage,
    (Case C_MigratedProject 
    WHEN True then 1
    WHEN False then 0
    ELSE C_MigratedProject
    END) as c_migrated_project,
    C_MigratedProjectID as c_migrated_project_id,
    C_ContractBusinessUnitPSCode as c_contract_business_unit_ps_code,
    C_ServiceProduct as c_service_product,
    C_ServiceType as c_service_type,
    C_SubmitHSReport as c_submit_hs_report,
    C_SustainabilityCertification as c_sustainability_certification,
    C_UnitofMeasure as c_unit_of_measure,
    C_UsableArea as c_usable_area,
    C_LocationName as c_location_name,
    C_GrossArea as c_gross_area,
    C_FitoutType as c_fitout_type,
    C_BuildingEnergyCode as c_building_energy_code,
    C_BusinessLine as c_business_line,
    C_RentableArea as c_rentable_area,
    C_RiskLevel as c_risk_level,
    C_LEEDCertofProject as c_leed_cert_of_project,
    C_LEEDBuilding as c_leed_building,
    C_LEEDProject as c_leed_project,
    EntityType as entity_type,
    C_AJD30 as c_ajd30,
    C_AJD29 as c_ajd29,
    C_AJD28 as c_ajd28,
    C_AJD26 as c_ajd26,
    C_AJD21 as c_ajd21,
    C_AJD27 as c_ajd27,
    C_AJD25 as c_ajd25,
    C_AJD24 as c_ajd24,
    C_AJD23 as c_ajd23,
    C_AJD22 as c_ajd22,
    C_AJD20 as c_ajd20,
    C_AJD19 as c_ajd19,
    C_AJD18 as c_ajd18,
    C_AJD16 as c_ajd16,
    C_AJD17 as c_ajd17,
    C_AJD15 as c_ajd15,
    C_AJD14 as c_ajd14,
    C_AJD13 as c_ajd13,
    C_AJD12 as c_ajd12,
    C_AJD11 as c_ajd11,
    C_AJD09 as c_ajd09,
    C_AJD10 as c_ajd10,
    C_AJD08 as c_ajd08,
    C_AJD07 as c_ajd07,
    C_AJD06 as c_ajd06,
    C_AJD05 as c_ajd05,
    C_AJD04 as c_ajd04,
    C_AJD03 as c_ajd03,
    C_AJD02 as c_ajd02,
    C_AJD01 as c_ajd01,
    C_RP30 as c_rp30,
    C_RP29 as c_rp29,
    C_RP28 as c_rp28,
    C_RP27 as c_rp27,
    C_RP26 as c_rp26,
    C_RP23 as c_rp23,
    C_RP13 as c_rp13,
    C_RP12 as c_rp12,
    C_RP10 as c_rp10,
    C_RP09 as c_rp09,
    C_RP08 as c_rp08,
    TrackStatus as track_status,
    (Case TrackStatusManuallySet 
    WHEN True then 1
    WHEN False then 0
    ELSE TrackStatusManuallySet
    END) as track_status_manually_set,
    Manager as Manager,
    TimeTrackingEffort as time_tracking_effort,
    TimeTrackingCost as time_tracking_cost,
    AlluserResourcesCount as All_user_Resources_Count,
    userResourcesCount as user_Resources_Count,
    EntityOwner as entity_owner,
    ScheduleSummary as schedule_summary,
    BudgetSummary as budget_summary,
    Risks as risks,
    'PROJECT' as dss_record_source,
    CAST('{refresh_date}' as TIMESTAMP) as dss_load_date,
    CAST('{refresh_date}' as TIMESTAMP) as dss_create_time,
    CAST('{refresh_date}' as TIMESTAMP) as dss_update_time,
    -- as dss_version,
    '' as clz_ally_financial_ally_activity,
    '' as clz_ally_financial_condition,
    '' as clz_ally_financial_current_phase,
    '' as clz_cna_lease_end_date,
    '' as clz_cna_project_type,
    '' as clz_customer_project_category_pl,
    '' as clz_customer_region_pl,
    '' as clz_dataload_identifier,
    (Case C_DataLoadToggle 
    WHEN True then 1
    WHEN False then 0
    ELSE C_DataLoadToggle
    END) as clz_dataload_toggle,
    C_DocumentFolder as clz_document_folder,
    C_EMEAAssetSubType as clz_emea_asset_subtype,
    C_EMEAAssetType as clz_emea_asset_type,
    '' as clz_leidos_dre, -- ( not found null in red)
    C_PeoplesoftLastUpdatedOn as clz_peoplesoft_last_updated_on_at,
    C_WorkflowType as clz_work_flow_type,
    --LastSyncedWithClarizenGo as last_synced_with_clarizen_go,
    --LinkToClarizenGo as link_to_clarizen_go,
    RateCard as rate_card,
    '' as sync_with_clarizen_go, --( not found null in red)
    C_CustomerProjectNumber as c_customer_project_number,
    C_ANZFinancialYear as clz_anz_financial_year,
    C_ANZProjectType as clz_anz_project_type,
    '' as clz_budget_template_deprecated, --( not found null in red)
    '' as clz_ryg_status, -- ( not found null in red)
    C_Stage as clz_stage,
    C_CustomerProjectStatus as clz_customer_project_status,
    C_CustomerProjectSubType as clz_customer_project_subtype,
    C_CustomerProjectType as clz_customer_project_type,
    C_CustomerBusinessUnit as clz_customer_business_unit,
    C_CustomerWorkType as clz_customer_work_type,
    C_CustomerRegion as clz_customer_region,
    (Case Billable 
        WHEN True then 1
        WHEN False then 0
        ELSE Billable
    END) as billable,
    ImageUrl as image_url,
    (Case Deliverable 
        WHEN True then 1
        WHEN False then 0
        ELSE Deliverable
    END) as deliverable,
    LastUpdatedBySystemOn as last_updated_by_system_on_at,
    C_CustomerProjectManager as c_customer_project_manager,
    C_IsWasteDivertedfromLandfill as C_IsWaste_Diverted_from_Landfill,
    '' as C_Volume_of_Saved_Water, --(need to confirm from RED)
    C_SalesforceID as C_SalesforceID,
    C_GCContractType as C_GCContract_Type,
    C_VirtualRealityTour as C_Virtual_Reality_Tour,
    C_AMPropertyType as C_AMProperty_Type,
    C_Energymodeldeveloped as C_Energy_model_developed,
    C_UnitofTargetEnergy as C_Unitof_Target_Energy,
    C_TargetEnergyUsepa as C_Target_Energy_Usepa,
    C_Certificationservicedeliveredby as C_Certification_service_deliveredby,
    C_CertifyingCompanyIfOther as C_Certifying_Company_IfOther,
    C_EnergyCode as C_Energy_Code,
    C_CertificationAchieved as C_Certification_Achieved,
    C_CarbonStudyImplemented as C_Carbon_Study_Implemented,
    C_TargetSustainabilityCertification as C_Target_Sustainability_Certification,
    '' as C_Water_Saved_During_Construction, --(need to confirm from RED)
    C_CarbonStudyConsultants as C_Carbon_Study_Consultants,
    C_TotalWasteforProject as C_Total_Wastefor_Project,
    C_WasteDivertedfromLandfill as C_Waste_Diverted_from_Landfill,
    C_VR360orOpenSpaceURL as C_VR360or_Open_SpaceURL,
    C_Link3 as C_Link3,
    C_Link2 as C_Link2,
    C_Link1 as C_Link1,
    C_MinorMarket as C_Minor_Market,
    C_MajorMarket as C_Major_Market,
    C_Procurementroute as C_Procurement_route,
    Importance as Importance,
    -- as dss_current_version, ( no history maintained)
    C_CarbonOffsetsPurchasedforProject as C_Carbon_Offsets_Purchased_for_Project,
    ProjectTagging as project_tagging,
    c_JLLProjectTypeOrgList as clz_jll_project_type_org_list,
    c_ServiceTypeOrgList as clz_service_type_org_list,
    BusinessAlignment as business_alignment,
    ConflictType as conflict_type,
    C_WorkItemType as C_workItem_type,
    C_AJD61 as c_ajd61,
    C_ClientSurveyTriggered as C_client_survey_triggered,
    C_PMSurveyTriggered as c_PM_survey_triggered,
    Mitigation as Mitigation,
    C_DesignedEnergySavings as c_designed_energy_savings,
    C_SustainabilityCertIfOther as c_sustainability_cert_if_other,
    C_AwardTargetAwardDate as c_award_target_award_date,
    ClosingNotes as closing_notes,
    C_CorrigoCustomerNumber as c_corrigo_customer_number,
    C_CorrigoId as c_corrigo_id,
    C_CorrigoIntegrationStatus as c_corrigo_integration_status,
    C_CorrigoProjectStartDate as c_corrigo_project_start_at
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project
where
    is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_client_mapping
spark.sql(""" 

with _unionedtables AS (
   SELECT DISTINCT
       p.client_id                           as client_id
      ,gt.id                                 as id           
      ,p.c_project_id                        as c_project_id
      ,''                                    as c_job_site_address
      ,gt.name                               as work_item_name
      ,'TASK'                                as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,r.client_id                           as r_client_id
      ,r.id                                  as r_id
      ,gt.project                            as project
      ,gt.sysid                              as sysid
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_generictask gt
     JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on gt.Project = p.id
LEFT JOIN {var_client_custom_db}.datavault_dbo_r_client_mapping r on gt.id = r.id

union all

   SELECT DISTINCT
       p.client_id                           as client_id
      ,m.id                                  as id           
      ,p.c_project_id                        as c_project_id
      ,''                                    as c_job_site_address
      ,m.name                                as work_item_name
      ,'MILESTONE'                           as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,r.client_id                           as r_client_id
      ,r.id                                  as r_id
      ,m.project                             as project
      ,m.sysid                               as sysid	  
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_milestone m
     JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on m.Project = p.id
LEFT JOIN {var_client_custom_db}.datavault_dbo_r_client_mapping r on m.id = r.id

union all

   SELECT DISTINCT
       p.client_id                           as client_id
      ,pr.id                                 as id           
      ,p.c_project_id                        as c_project_id
      ,''                                    as c_job_site_address
      ,pr.name                               as work_item_name
      ,'PROGRAM'                             as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,r.client_id                           as r_client_id
      ,r.id                                  as r_id
      ,pr.project                            as project
      ,pr.sysid                              as sysid		  
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_program pr
     JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on pr.Project = p.id
LEFT JOIN {var_client_custom_db}.datavault_dbo_r_client_mapping r on pr.id = r.id

union all

   SELECT DISTINCT
       substring(pj.C_Customer,charindex('/',pj.C_Customer,2)+1,50) as client_id 
      ,pj.id                                 as id           
      ,pj.C_ProjectID                        as c_project_id
      ,pj.C_JobSiteAddress                   as c_job_site_address
      ,pj.name                               as work_item_name
      ,'Project'                             as dss_record_source
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_at
      ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_at
      ,p.client_id                           as r_client_id
      ,p.id                                  as r_id
      ,pj.project                            as project
      ,pj.sysid                              as sysid	
     FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project pj
LEFT JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where upper(dss_record_source) = 'PROJECT') p on pj.id = p.id
)

       MERGE INTO {var_client_custom_db}.datavault_dbo_r_client_mapping r
            USING _unionedtables ut
               ON r.id = ut.r_id
WHEN MATCHED THEN 
       UPDATE SET r.sys_id             = ut.sysid
                 ,r.project            = ut.project       
                 ,r.client_id          = ut.client_id
                 ,r.id                 = ut.id
                 ,r.c_project_id       = ut.c_project_id
                 ,r.c_job_site_address = ut.c_job_site_address
                 ,r.work_item_name     = ut.work_item_name
                 ,r.dss_record_source  = ut.dss_record_source
                 ,r.dss_load_at        = ut.dss_load_at
                 ,r.dss_create_at      = ut.dss_create_at
WHEN NOT MATCHED AND IF(ISNULL(ut.client_id),'ABC',ut.client_id) <> IF(ISNULL(ut.r_client_id),'ABC',ut.r_client_id)
THEN INSERT (sys_id, project, client_id, id, c_project_id, c_job_site_address, work_item_name, dss_record_source, dss_load_at, dss_create_at) 
     VALUES (ut.sysid, ut.project, ut.client_id, ut.id, ut.c_project_id, ut.c_job_site_address, ut.work_item_name, ut.dss_record_source, ut.dss_load_at, ut.dss_create_at)

""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_clz_directory_contact_project_links
spark.sql(f""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_clz_directory_contact_project_links AS
select
	PL.id,
	SPD.client_id as client_id,
	ExternalID as external_id,
	EntityType as entity_type,
	PL.Project as project,
	C_DirectoryContacts as clz_directory_contacts,
	C_RoleinSpend as clz_role_in_spend,
	CreatedBy as created_by,
	cast(CreatedOn as timestamp) as created_at,
	LastUpdatedBy as last_updated_by,
	cast(LastUpdatedOn as timestamp) as last_updated_on_at,
	'DIRECTORYCONTACTPROJECTLINK' as dss_record_source,
	CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
	CAST('{refresh_date}' as TIMESTAMP) as dss_start_at,
	CAST('{refresh_date}' as TIMESTAMP) as dss_create_at,
    (Case C_SyncRolestoSpend 
    WHEN True then 1
    WHEN False then 0
    ELSE C_SyncRolestoSpend
    END) as Clz_Sync_Roles_to_Spend
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_contactprojectlink PL
left join 
    {var_client_custom_db}.datavault_dbo_s_project_details SPD
    on PL.project = SPD.project 
where
    PL.is_deleted = FALSE
""")

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_c_directory_contacts
spark.sql(""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.datavault_dbo_r_c_directory_contacts AS
SELECT
    id                                       as id,
    Name                                     as name,
    C_EmailAddress                           as c_email_address,
    ExternalID                               as external_id,
    SYSID                                    as sys_id,
    C_MobilePhone                            as c_mobile_phone,
    C_LastName                               as c_lastname,
    C_FirstName                              as c_firstname,  
    C_Address                                as c_address,
    C_BusinessPhone                          as c_business_phone,
    C_FaxNumber                              as c_fax_number, 
    C_JobTitle                               as c_job_title,
    Description                              as description,
    C_Service                                as c_service,
    C_TypeofContact                          as c_type_of_contact,
    C_Notes                                  as c_notes,
    C_Company                                as c_company,
    C_CompanyVendor                          as c_company_vendor,
    C_CompanyVendorTC                        as c_company_vendor_tc,
    (Case C_AddedfromUser 
        WHEN True then 1
        WHEN False then 0
        ELSE C_AddedfromUser
    END)                                     as c_added_from_user,
    C_Comments                               as c_comments,
    C_Customer                               as c_customer,
    ImageUrl                                 as image_url,
    cast(EmailsCount as int)                 as emails_count,
    EntityOwner                              as entity_owner,
    EntityType                               as entity_type,
    CurrencyExchangeDate                     as currency_exchange_date,
    CurrencyExchangeDateManuallySet          as currency_exchange_date_manually_set,
    CreatedBy                                as created_by,
    CreatedOn                                as created_on,
    LastUpdatedBy                            as last_updated_by,
    LastUpdatedOn                            as last_updated_on,
    C_State                                  as clz_state,
    C_User                                   as clz_user,
    'DIRECTORY CONTACTS'                     as dss_record_source,
    CAST('{refresh_date}' as TIMESTAMP)      as dss_load_at,
    CAST('{refresh_date}' as TIMESTAMP)      as dss_update_at
from 
  (
    SELECT 
      id, 
      CreatedBy, 
      CreatedOn, 
      LastUpdatedBy, 
      LastUpdatedOn, 
      EntityType, 
      Name, 
      Description, 
      ExternalID, 
      SYSID, 
      ImageUrl, 
      EntityOwner, 
      CurrencyExchangeDate, 
      EmailsCount, 
      CurrencyExchangeDateManuallySet, 
      C_JobTitle, 
      C_CompanyVendorTC, 
      C_State, 
      C_User, 
      C_MobilePhone, 
      C_LastName, 
      C_FirstName, 
      C_FaxNumber, 
      C_EmailAddress, 
      C_Service, 
      C_TypeofContact, 
      C_Notes, 
      C_Company, 
      C_CompanyVendor, 
      C_AddedfromUser, 
      C_Comments, 
      C_Address, 
      C_BusinessPhone, 
      C_Customer, 
      is_deleted, 
      row_number() over ( partition by id order by lastupdatedon desc) as rn 
from 
      {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_directorycontacts ) cdc 
where 
    cdc.rn = 1 
and 
    cdc.is_deleted = FALSE

""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_clz_spend_role
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_r_clz_spend_role AS
SELECT 
	id,
	ExternalID as external_id,
	SYSID as sys_id,
	EntityType as entity_type,
	EntityOwner as entity_owner,
	Name as name,
	C_RoleinSpendCode as clz_role_in_spend_code,
	Description as description,
	ImageUrl as imageurl,
	EmailsCount as emails_count,
	CurrencyExchangeDate as currency_exchange_date_at,
	CurrencyExchangeDateManuallySet as currency_exchange_date_manually_set,
	CreatedBy as created_by,
	cast(CreatedOn as timestamp) as created_at,
	LastUpdatedBy as last_updated_by,
	cast(LastUpdatedOn as timestamp) as last_updated_on_at,
	'SPEND ROLE' as dss_record_source,
	CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
	CAST('{refresh_date}' as TIMESTAMP) as dss_create_at
from 
(select 
		id, 
		CreatedBy, 
		CreatedOn, 
		LastUpdatedBy, 
		LastUpdatedOn, 
		EntityType, 
		Name, 
		Description, 
		ExternalID, 
		SYSID, 
		ImageUrl, 
		EntityOwner, 
		CurrencyExchangeDate, 
		EmailsCount, 
		CurrencyExchangeDateManuallySet, 
		C_RoleinSpendCode,
		is_deleted,
		row_number() over (partition by id order by lastupdatedon desc) as rn
	from  
		{var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_spendrole ) CER 
where 
	CER.rn = 1 and 
	CER.is_deleted = FALSE
order by
	id, 
	CreatedBy, 
	CreatedOn, 
	LastUpdatedBy, 
	LastUpdatedOn, 
	EntityType, 
	Name, 
	Description, 
	ExternalID, 
	SYSID, 
	ImageUrl, 
	EntityOwner, 
	CurrencyExchangeDate, 
	EmailsCount, 
	CurrencyExchangeDateManuallySet, 
	C_RoleinSpendCode
 """.format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_c_job_site
spark.sql(f""" 
CREATE OR REPLACE TABLE
    {var_client_custom_db}.datavault_dbo_s_c_job_site AS
SELECT
    cj.id, --(hash key replacement)
    spa.client_id                         as client_id,
    CreatedBy                             as created_by,
    cast(CreatedOn as timestamp)          as created_on,
    LastUpdatedBy                         as last_updated_by,
    cast(LastUpdatedOn as timestamp)      as last_updated_on,
    EntityType                            as entity_type,
    Name                                  as name,
    Description                           as description,
    ExternalID                            as external_id,
    SYSID                                 as sys_id,
    ImageUrl                              as image_url,
    EntityOwner                           as entity_owner,
    CurrencyExchangeDate                  as currency_exchange_date,
    cast(EmailsCount as int)              as emails_count,
    CurrencyExchangeDateManuallySet       as currency_exchange_date_manually_set,
    C_Address1                            as c_address_1,
    C_ClientPropertyCode                  as c_client_property_code,
    C_StateProvince                       as c_state_province,
    C_Address2                            as c_address_2,
    C_RequestedbyUser                     as c_requested_by_user,
    cj.C_City                             as c_city,
    C_JobSiteAddressE1                    as c_job_site_address_e1,
    C_ClientRegion                        as c_client_region,
    C_Country                             as c_country,
    C_MDMID                               as c_mdm_id,
    C_OVCPID                              as c_ovcp_id,
    C_PropertyTypeE1Code                  as c_property_type_e1_code,
    C_PropertyType                        as c_property_type,
    C_PropertySubType                     as c_property_sub_type,
    C_PostalCode                          as c_postal_code,
    cj.C_Longitude                        as c_longitude,
    cj.C_Latitude                         as c_latitude,
    'JOBSITE'                             as dss_record_source,
    CAST('{refresh_date}' as TIMESTAMP)   as dss_load_date,
    CAST('{refresh_date}' as TIMESTAMP)   as dss_start_date,
    CAST('{refresh_date}' as TIMESTAMP)   as dss_create_time,
    C_AddressSearch                       as clz_address_search,
    C_OVCID                               as clz_ovc_id                   
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_jobsite CJ
left join
    {var_client_custom_db}.datavault_dbo_s_project_address spa
    on CJ.id = spa.c_job_site_address
where
    is_deleted = FALSE
""")

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_clz_directory_contact_project_links
spark.sql(f""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_clz_directory_contact_project_links AS
select
	PL.id,
	SPD.client_id as client_id,
	ExternalID as external_id,
	EntityType as entity_type,
	PL.Project as project,
	C_DirectoryContacts as clz_directory_contacts,
	C_RoleinSpend as clz_role_in_spend,
	CreatedBy as created_by,
	cast(CreatedOn as timestamp) as created_at,
	LastUpdatedBy as last_updated_by,
	cast(LastUpdatedOn as timestamp) as last_updated_on_at,
	'DIRECTORYCONTACTPROJECTLINK' as dss_record_source,
	CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
	CAST('{refresh_date}' as TIMESTAMP) as dss_start_at,
	CAST('{refresh_date}' as TIMESTAMP) as dss_create_at,
    (Case C_SyncRolestoSpend 
    WHEN True then 1
    WHEN False then 0
    ELSE C_SyncRolestoSpend
    END) as Clz_Sync_Roles_to_Spend
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_contactprojectlink PL
left join 
    {var_client_custom_db}.datavault_dbo_s_project_details SPD
    on PL.project = SPD.project 
where
    PL.is_deleted = FALSE
""")

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_project_milestones
spark.sql(""" 
CREATE OR REPLACE TABLE 
    {var_client_custom_db}.datavault_dbo_s_project_milestones as
 SELECT 
     P.ID -- (ID added in place of hashkey: Project Hashkey created from ID)
    ,SUBSTRING_INDEX(C_Customer, '/', -1) as client_id
    ,ExternalID                           as external_id
    ,AdditionalComments                   as additional_comments
    ,ActualStartDate                      as actual_start_date
    ,ActualEndDate                        as actual_end_date
    ,BaselineDueDate                      as baseline_due_date
    ,BaselineDueDateVariance              as baseline_due_date_variance
    ,BaselineDuration                     as baseline_duration
    ,BaselineDurationVariance             as baseline_duration_variance
    ,BaselineStartDate                    as baseline_start_date
    ,BaselineStartDateVariance            as baseline_start_date_variance
    ,BaselineCreationDate                 as baseline_creation_date
    ,''                                   as c_projected_end_date -- Confirmed Not Required
    ,DueDate                              as due_date
    ,Duration                             as duration
    ,ActualDuration                       as actual_duration
    ,CASE WHEN UPPER(DurationManuallySet) = 'FALSE' THEN '0'
          WHEN UPPER(DurationManuallySet) = 'TRUE'  THEN '1'
     END                                  as duration_manually_set
    ,DurationVariance                     as duration_variance
    ,PercentCompleted                     as percent_completed
    ,StartDate                            as start_date
    ,FinancialStart                       as financial_start
    ,FinancialEnd                         as financial_end
    ,FinancialOldStartDate                as financial_old_start_date_at
    ,'PROJECT'                            as dss_record_source
    ,CAST('{refresh_date}' as TIMESTAMP)  as dss_load_date
    ,CAST('{refresh_date}' as TIMESTAMP)  as dss_create_time
    ,CAST('{refresh_date}' as TIMESTAMP)  as dss_update_time        
    ,LastReplanningDate                   as last_replanning_date
    ,ReportableEndDate                    as reportable_end_date
    ,ResourceRateDate                     as resource_rate_date
	,to_timestamp(C_InitiationDate)       as clz_initiation_date
    ,LastUpdatedOn                        as last_updated_at
    ,LastUpdatedBy                        as last_updated_by
    ,LastUpdatedBySystemOn                as last_updated_by_system_on_at
    ,CommittedDate                        as Committed_Date
    ,StatusChangedOn                      as status_changed_on
    ,EstimatedStartDate                   as estimated_start_date
    ,EstimatedEndDate                     as estimated_end_date
    ,EstimatedDuration                    as estimated_duration
    ,C_LatestEndDate                      as c_latest_end_date
    ,C_TargetOccupancyDate                as c_target_occupancy_date
    --,null                                  as c_target_occupancy_date
 FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project P
 JOIN (SELECT DISTINCT ID FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project WHERE ID IS NOT NULL OR ID <> '' ORDER BY ID) H ON P.ID = H.ID -- Emulating H_Project Table Join
WHERE UPPER(Is_Deleted) = 'FALSE'
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_work_item
spark.sql(f""" 
CREATE OR REPLACE TABLE 
    {var_client_custom_db}.datavault_dbo_s_work_item as
 SELECT
    NVL(UPPER(wi.SYSID),'ABC')          as sys_id    -- (SYSID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
   ,NVL(UPPER(wi.Project),'ABC')        as project   -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
   ,ht.client_id                        as client_id -- (Client_ID used in hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
   ,wi.ExternalID                       as external_id
   ,wi.id                               as id
   ,wi.is_deleted                       as is_deleted
   ,wi.ActualStartDate                  as actual_start_date
   ,wi.ActualEndDate                    as actual_end_date
   ,wi.BaselineDueDate                  as baseline_due_date
   ,wi.BaselineDueDateVariance          as baseline_due_date_variance
   ,wi.BaselineDuration                 as baseline_duration
   ,wi.BaselineDurationVariance         as baseline_duration_variance
   ,wi.BaselineStartDate                as baseline_start_date
   ,wi.BaselineStartDateVariance        as baseline_start_date_variance
   ,wi.BaselineCreationDate             as baseline_creation_date
   ,wi.DueDate                          as due_date
   ,wi.DueDateVariance                  as due_date_variance
   ,wi.Duration                         as duration
   ,wi.ActualDuration                   as actual_duration
   ,wi.DurationManuallySet              as duration_manually_set
   ,wi.DurationVariance                 as duration_variance
   ,wi.PercentCompleted                 as percent_completed
   ,wi.StartDate                        as start_date
   ,wi.StartDateVariance                as start_date_variance
   ,wi.StateProvince                    as state_province
   ,wi.GeographicalRegion               as geographical_region
   ,wi.Country                          as country
   ,wi.C_MilestoneNumber                as c_milestone_number
   ,''                                  as clz_cost_code -- Confirmed not required
   ,'WORKITEM'                          as dss_record_source
   ,CAST('{refresh_date}' as TIMESTAMP) as dss_load_date
   ,CAST('{refresh_date}' as TIMESTAMP) as dss_create_time
   ,CAST('{refresh_date}' as TIMESTAMP) as dss_update_time  
   ,wi.LastUpdatedOn                    as last_updated_on
   ,wi.LastUpdatedBySystemOn            as last_updated_by_system_on_at
   ,wi.LastUpdatedBy                    as last_updated_by
     FROM {var_client_custom_db}.stage_clarizen_dbo_workitem    wi 
     JOIN (SELECT DISTINCT wi.Project, wi.SYSID, 'WORKITEM' as record_source, rc.client_id 
           FROM {var_client_custom_db}.stage_clarizen_dbo_workitem wi
      LEFT JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where UPPER(dss_record_source) = 'PROJECT') rc ON rc.id = wi.Project) ht -- Emulating H_WorkItem Join
       ON NVL(UPPER(wi.Project),'ABC') = NVL(UPPER(ht.Project),'ABC')
      AND NVL(UPPER(wi.SYSID),'ABC')   = NVL(UPPER(ht.SYSID),'ABC') 
    WHERE Is_Deleted = '0'
""")

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_work_item_details
spark.sql(f""" 
CREATE OR REPLACE TABLE 
    {var_client_custom_db}.datavault_dbo_s_work_item_details as
 SELECT
     NVL(UPPER(wi.SYSID),'ABC')          as sys_id    -- (SYSID added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
    ,NVL(UPPER(wi.Project),'ABC')        as project   -- (Project added in place of hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
    ,ht.client_id                        as client_id -- (Client_ID used in hashkey: Work Item Hashkey created from Client_ID, Project, and Sys_ID)
    ,''                                  as c_customer_project_number -- Confirmed Not Required
    ,wi.C_Customertask                   as c_customer_task
    ,''                                  as c_data_load_dentifier -- Confirmed Not Required
    ,wi.Name                             as name
    ,wi.ParentProject                    as parent_project
    ,wi.Parent                           as parent
    ,wi.BudgetStatus                     as budget_status
    ,wi.LikesCount                       as likes_count
    ,wi.PostsCount                       as posts_count
    ,wi.Phase                            as phase
    ,wi.LastUpdatedBy                    as last_updated_by
    ,wi.LastUpdatedOn                    as last_updated_on
    ,wi.C_ServiceProductE1Code           as c_service_product_e1_code
    ,wi.C_ProgramType                    as c_program_type
    ,wi.C_ScopeStatus                    as c_scope_status
    ,wi.C_MilestoneNumber                as c_milestone_number
    ,wi.C_RiskRegistryScore              as c_risk_registry_score
    ,wi.C_RiskRegisterStatus             as c_risk_register_status
    ,wi.C_ReportingName                  as c_reporting_name
    ,wi.C_ReportingId                    as c_reporting_id
    ,wi.C_ScheduleStatus                 as c_schedule_status
    ,wi.C_Vendor                         as c_vendor
    ,wi.C_RuntimeId                      as c_runtime_id
    ,wi.C_InternalType                   as c_internal_type
    ,wi.C_ProjectBuildingSize            as c_project_building_size
    ,wi.C_ServiceProduct                 as c_service_product
    ,wi.EntityType                       as entity_type
    ,''                                  as c_service_type -- Confirmed Not Required
    ,wi.C_AJD30
    ,wi.C_AJD29
    ,wi.C_AJD28
    ,wi.C_AJD26
    ,wi.C_AJD21
    ,wi.C_AJD27
    ,wi.C_AJD25
    ,wi.C_AJD24
    ,wi.C_AJD23
    ,wi.C_AJD22
    ,wi.C_AJD20
    ,wi.C_AJD19
    ,wi.C_AJD18
    ,wi.C_AJD16
    ,wi.C_AJD17
    ,wi.C_AJD15
    ,wi.C_AJD14
    ,wi.C_AJD13
    ,wi.C_AJD12
    ,wi.C_AJD11
    ,wi.C_AJD09
    ,wi.C_AJD10
    ,wi.C_AJD08
    ,wi.C_AJD07
    ,wi.C_AJD06
    ,wi.C_AJD05
    ,wi.C_AJD04
    ,wi.C_AJD03
    ,wi.C_AJD02
    ,wi.C_AJD01
    ,wi.C_RP30
    ,wi.C_RP29
    ,wi.C_RP28
    ,wi.C_RP27
    ,wi.C_RP26
    ,wi.C_RP23
    ,wi.C_RP13
    ,wi.C_RP12
    ,wi.C_RP10
    ,wi.C_RP09
    ,wi.C_RP08
    ,wi.CreatedBy                        as created_by
    ,wi.CreatedOn                        as created_on
    ,wi.Description                      as description
    ,'WORKITEM'                          as dss_record_source
    ,CAST('{refresh_date}' as TIMESTAMP) as dss_load_date 
    ,CAST('{refresh_date}' as TIMESTAMP) as dss_create_time 
    ,CAST('{refresh_date}' as TIMESTAMP) as dss_update_time 
    ,wi.TrackStatus                      as track_status
    ,wi.TrackStatusManuallySet           as track_status_manually_set
    ,wi.InternalStatus                   as internal_status
    ,wi.billable                         as billable
    ,wi.deliverable                      as deliverable
    ,wi.LastUpdatedBySystemOn            as last_updated_by_system_on_at
     FROM {var_client_custom_db}.stage_clarizen_dbo_workitem    wi
     JOIN (SELECT DISTINCT wi.Project, wi.SYSID, 'WORKITEM' as record_source, rc.client_id 
           FROM {var_client_custom_db}.stage_clarizen_dbo_workitem wi
      LEFT JOIN (SELECT * FROM {var_client_custom_db}.datavault_dbo_r_client_mapping where UPPER(dss_record_source) = 'PROJECT') rc ON rc.id = wi.Project) ht -- Emulating H_WorkItem Join
       ON NVL(UPPER(wi.Project),'ABC') = NVL(UPPER(ht.Project),'ABC')
      AND NVL(UPPER(wi.SYSID),'ABC')   = NVL(UPPER(ht.SYSID),'ABC') 
    WHERE Is_Deleted = '0'
""")

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_project_customer
spark.sql(""" 
CREATE OR REPLACE TABLE 
    {var_client_custom_db}.datavault_dbo_s_project_customer as
 SELECT 
     P.ID -- (ID added in place of hashkey: Project Hashkey created from ID)
    ,SUBSTRING_INDEX(C_Customer, '/', -1) as client_id
    ,C_SpendCustomerID                    as c_spend_customer_id
    ,C_SpendIntegrationStatus             as c_spend_integration_status
    ,CASE WHEN UPPER(C_SpendProjectIntegration)      = 'FALSE' THEN '0'
          WHEN UPPER(C_SpendProjectIntegration)      = 'TRUE'  THEN '1'
     END                                  as c_spend_project_integration
    ,CASE WHEN UPPER(C_Customertask)                 = 'FALSE' THEN '0'
          WHEN UPPER(C_Customertask)                 = 'TRUE'  THEN '1'
     END                                  as c_customer_task
    ,C_CustomerType                       as c_customer_type
    ,CASE WHEN UPPER(C_Visibletocustomer)            = 'FALSE' THEN '0'
          WHEN UPPER(C_Visibletocustomer)            = 'TRUE'  THEN '1'
     END                                  as c_visible_to_customer
    ,C_CustomerAdditionalFields           as c_customer_additional_fields
    ,C_CustomerList                       as c_customer_list
    ,C_CCCustomerProjectReferenceLink     as c_cccustomer_project_reference_link
    ,C_Customer                           as c_customer
    ,C_CustomerProjectCategory            as c_customer_project_category
    ,''                                   as c_funding_source_work_place_solutions -- Confirmed Not Required
    ,C_PeoplesoftLastUpdatedOn            as c_peoplesoft_last_updated_on
    ,CASE WHEN UPPER(C_DataLoadToggle)               = 'FALSE' THEN '0'
          WHEN UPPER(C_DataLoadToggle)               = 'TRUE'  THEN '1'
     END                                  as c_data_load_toggle
    ,C_Industry                           as c_industry
    ,C_PeoplesoftPropertyType             as c_peoplesoft_property_type
    ,C_PeoplesoftIntegrationStatus        as c_peoplesoft_integration_status
    ,CASE WHEN UPPER(C_PeoplesoftProjectIntegration) = 'FALSE' THEN '0'
          WHEN UPPER(C_PeoplesoftProjectIntegration) = 'TRUE'  THEN '1'
     END                                  as c_peoplesoft_project_integration
    ,C_PeoplesoftProjectID                as c_peoplesoft_project_id
    ,'PROJECT'                            as dss_record_source
    ,CAST('{refresh_date}' as TIMESTAMP)  as dss_load_date
    ,CAST('{refresh_date}' as TIMESTAMP)  as dss_create_time
    ,CAST('{refresh_date}' as TIMESTAMP)  as dss_update_time  
    ,CustomerCount                        as customer_count
    ,LastUpdatedOn                        as last_updated_at
    ,LastUpdatedBy                        as last_updated_by
    ,LastUpdatedBySystemOn                as last_updated_by_system_on_at
 FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project P
 JOIN (SELECT DISTINCT ID FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project WHERE ID IS NOT NULL OR ID <> '' ORDER BY ID) H ON P.ID = H.ID -- Emulating H_Project Table Join
WHERE UPPER(Is_Deleted) = 'FALSE'
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_project_financials
spark.sql(""" 
CREATE OR REPLACE TABLE 
	{var_client_custom_db}.datavault_dbo_s_project_financials as
 SELECT 
     P.ID -- (ID added in place of hashkey: Project Hashkey created from ID)
    ,SUBSTRING_INDEX(C_Customer, '/', -1)  as client_id
	,BudgetCostCAPEX                       as budget_cost_capex
	,BudgetCostLR                          as budget_cost_lr 
	,BudgetCostNLR                         as budget_cost_nlr
	,BudgetCostOPEX                        as budget_cost_opex
    ,CapexActualYTD                        as capex_actual_ytd
	,OpexActualYTD                         as opex_actual_ytd
	,CapexBudgetYTD                        as capex_budget_ytd
	,OpexBudgetYTD                         as opex_budget_ytd
	,C_AdditionalForecastedCosts           as c_additional_forecasted_costs
	,C_AdditionalForecastedCostsExternal   as c_additional_forecasted_costs_external
	,C_AdditionalForecastedCostsRollup     as c_additional_forecasted_costs_rollup
	,C_AnticipatedCosttoComplete           as c_anticipated_cost_to_complete 
	,C_ApprovedChangesandTransferRollup    as c_approved_changes_and_transfer_rollup
	,C_ApprovedChangesandTransfers         as c_approved_changes_and_transfers
	,C_ApprovedChangesandTransfersExternal as c_approved_changes_and_transfers_external
	,C_ApprovedCommitments                 as c_approved_commitments
	,C_ApprovedCommitmentsAllowance        as c_approved_commitments_allowance
	,C_ApprovedCommitmentsExternal         as c_approved_commitments_external
	,C_ApprovedCommitmentsRollup           as c_approved_commitments_rollup
	,C_CAPEXP                              as c_capexp
	,''                                    as c_contracted_amount -- Confirmed Not Required
	,C_CostType                            as c_cost_type
	,''                                    as c_total_invoices_allowance_rollup -- Confirmed Not Required
	,C_TotalApprovedInvoicesRollup         as c_total_approved_invoices_rollup 
	,C_OriginalBudget                      as c_original_budget
	,C_OriginalBudgetAllowance             as c_original_budget_allowance
	,C_OriginalBudgetExternal              as c_original_budget_external
	,C_OriginalBudgetRollup                as c_original_budget_rollup
	,C_PendingChangesandTransfers          as c_pending_changes_and_transfers
	,C_PendingChangesandTransfersExternal  as c_pending_changes_and_transfers_external
	,C_PendingChangesandTransfersRollup    as c_pending_changes_and_transfers_rollup
	,C_PendingCommitments                  as c_pending_commitments
	,C_PendingCommitmentsAllowance         as c_pending_commitments_allowance
	,C_PendingCommitmentsExternal          as c_pending_commitments_external
	,C_PendingCommitmentsRollup            as c_pending_commitments_rollup
	,C_ProjectedTotalCommitments           as c_projected_total_commitments
	,C_ReimbursableInvoiceAmountRollup     as c_reimbursable_invoice_amount_rollup
	,C_RemainingBalancetoInvoice           as c_remaining_balance_to_invoice
	,C_TotalApprovedBudget                 as c_total_approved_budget
	,C_TotalApprovedInvoices               as c_total_approved_invoices 
	,C_TotalInvoices                       as c_total_invoices
	,C_TotalInvoicesAllowance              as c_total_invoices_allowance
	,C_TotalInvoicesExternal               as c_total_invoices_external
	,C_TotalNoAllowanceSection             as c_total_no_allowance_section
	,C_TotalPendingInvoices                as c_total_pending_invoices
	,C_TotalProjectedBudget                as c_total_projected_budget
	,C_VarianceofBudgettoCost              as c_variance_of_budget_to_cost
	,ActualCost                            as actual_cost
	,PlannedBudget                         as planned_budget
	,CostVariance                          as cost_variance
	,TimeTrackingBilling                   as time_tracking_billing
	,EarnedValue                           as earned_value
	,PlannedRevenue                        as planned_revenue
	,Profitability                         as profitability
	,PercentProfitability                  as percent_profitability
	,TotalEstimatedCost                    as total_estimated_cost
	,ActualCostOPEX                        as actual_cost_opex
	,ActualCostCAPEX                       as actual_cost_capex
    ,ActualCostNLR                         as actual_cost_nlr
	,ActualCostNLROpex                     as actual_cost_nlr_opex
	,ActualCostNLRCapex                    as actual_cost_nlr_capex
	,ActualCostLR                          as actual_cost_lr
	,RemainingForecastCostNLR              as remaining_forecast_cost_nlr
 	,RemainingBudget                       as remaining_budget
	,BudgetVariance                        as budget_variance
	,C_BaseInvoiceAmountRollup             as clz_base_invoice_amount_rollup 
    ,'PROJECT'                             as dss_record_source
    ,CAST('{refresh_date}' as TIMESTAMP)   as dss_load_date
    ,CAST('{refresh_date}' as TIMESTAMP)   as dss_create_time
    ,CAST('{refresh_date}' as TIMESTAMP)   as dss_update_time  
	,C_TaxAmountRollup                     as clz_tax_amount_rollup
 	,LastUpdatedOn                         as last_updated_at
	,LastUpdatedBy                         as last_updated_by
	,LastUpdatedBySystemOn                 as last_updated_by_system_on_at
    ,''                                    as clz_pending_changes_and_transfers_cap -- Cap & Exp fields below deprecated in Clarizen as per email 08/16
    ,''                                    as clz_approved_changes_and_transfers_exp
    ,''                                    as clz_approved_commitments_exp
    ,''                                    as clz_original_budget_exp
    ,''                                    as clz_original_budget_cap
    ,''                                    as clz_approved_commitments_cap
    ,''                                    as clz_pending_commitments_cap
    ,''                                    as clz_pending_commitments_exp
    ,''                                    as clz_approved_changes_and_transfers_cap
    ,''                                    as clz_total_approved_budget_cap
    ,''                                    as clz_additional_forecasted_costs_cap
    ,''                                    as clz_total_approved_budget_exp
    ,''                                    as clz_anticipated_cost_to_complete_cap
    ,''                                    as clz_anticipated_cost_to_complete_exp
    ,''                                    as clz_total_approved_invoices_exp
    ,''                                    as clz_total_projected_budget_exp
    ,''                                    as clz_variance_of_budget_to_cost_cap
    ,''                                    as clz_total_projected_budget_cap
    ,''                                    as clz_total_pending_invoices_cap
    ,''                                    as clz_remaining_balance_to_invoice_cap
    ,''                                    as clz_total_invoices_exp
    ,''                                    as clz_total_invoices_cap
    ,''                                    as clz_remaining_balance_to_invoice_exp
    ,''                                    as clz_total_pending_invoices_exp
    ,''                                    as clz_total_approved_invoices_cap
    ,''                                    as clz_projected_total_commitments_cap
    ,''                                    as clz_variance_of_budget_to_cost_exp
    ,''                                    as clz_additional_forecasted_costs_exp
    ,''                                    as clz_pending_changes_and_transfers_exp
    ,''                                    as clz_projected_total_commitments_exp
    ,C_ClientAssetValues                   as clz_Client_Asset_Values
    ,C_ClientGrouping                      as clz_Client_Grouping
    ,C_ClientWBSCode                       as clz_Client_WBS_Code
	,Funding                               as funding
    ,FundingGap                            as fundin_gap
    ,Allocation                            as allocation
    ,SubAllocation                         as sub_allocation
    ,TotalAllocation                       as total_allocation
    ,Unallocated                           as unallocated
    ,UnusedAllocation                      as unused_allocation
    ,FundingAvailable                      as funding_available
    ,FundingVariance                       as funding_variance
    ,FundingRemaining                      as funding_remaining
    ,SuggestedRevenueNLR                   as suggested_revenue_NLR
    ,SuggestedActualRevenueNLR             as suggested_actual_revenue_NLR
 FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project P
 JOIN (SELECT DISTINCT ID FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project WHERE ID IS NOT NULL OR ID <> '' ORDER BY ID) H ON P.ID = H.ID -- Emulating H_Project Table Join
WHERE UPPER(Is_Deleted) = 'FALSE'
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_Project_financial_Details
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_Project_financial_Details AS
select 
    id,
    SUBSTRING_INDEX(C_Customer, '/', -1) as client_id,
    '' as c_cost_per_area_deprecated,
    CostCurrencyType as cost_currency_type,
    C_CostPerArea as c_cost_per_area,
    C_GlobalCurrency as c_global_currency,
    C_LocalCurrency as c_local_currency,
    C_ExchangeRate as c_exchange_rate,
    C_ProjectTemplate as c_project_template,
    C_CostCodeSource as c_cost_code_source,
    'PROJECT' as dss_record_source,
    CAST('{refresh_date}' as TIMESTAMP) as dss_load_date,
    CAST('{refresh_date}' as TIMESTAMP) as dss_create_time,
    CAST('{refresh_date}' as TIMESTAMP) as dss_update_time,
    --as dss_version, (no history maintained)
    cast(LastUpdatedOn as timestamp) as last_updated_at,
    LastUpdatedBy as last_updated_by,
    cast(LastUpdatedBySystemOn as timestamp)  as last_updated_by_system_on_at,
    C_ClientAssetValues as Clz_Client_Asset_Values,
    C_ClientGrouping as Clz_Client_Grouping,
    C_ClientWBSCode as Clz_Client_WBS_Code,
    --as dss_current_version, (no history maintained)
    C_FinancialSubmissionDate as clz_financial_submission_at,
    C_FinancialSubmittedBy as clz_financial_submitted_by,
    C_FinanceSubmissionComments as clz_finance_submission_comments,
    C_ScannedInvoiceURL as clz_scanned_invoice_url
 FROM 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_project
where
    is_deleted = FALSE
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db,var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_s_clz_project_status_indicator
spark.sql(f""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_clz_project_status_indicator AS

with _clz_notes AS (
    Select ID, LTRIM(RTRIM(regexp_replace(regexp_replace(regexp_replace(C_Notes, '<.*?>', ' '),'&#.*?;','-'),'&.*?;',''))) as clz_notes_reg
    FROM {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_projectstatsindicator
)

select
    PSI.id                              as id,
    SPD.client_id                       as client_id,
    PSI.ExternalID                      as external_id,
    PSI.SYSID                           as sys_id, 
    PSI.Name                            as name, 
    PSI.Description                     as description, 
    EmailsCount                         as emails_count,
    ImageUrl                            as image_url,
    EntityOwner                         as entity_owner, 
    EntityType                          as entity_type, 
    C_ProjectStatusIndicator            as clz_project_status_indicator, 
    C_Status                            as clz_status, 
    C_Type                              as clz_type,
    {var_client_custom_db}.fn_ProcessHTMLText(clz_notes_reg) as clz_notes,
    CurrencyExchangeDate                as currency_exchange_at, 
    CurrencyExchangeDateManuallySet     as currency_exchange_date_manually_set, 
    CreatedBy                           as created_by, 
    CreatedOn                           as created_at, 
    LastUpdatedBy                       as last_updated_by, 
    LastUpdatedOn                       as last_updated_at, 
    'PROJECT INDICATOR'                 as dss_record_source, 
    CAST('{refresh_date}' as TIMESTAMP) as dss_load_at, 
    CAST('{refresh_date}' as TIMESTAMP) as dss_start_at, 
    CAST('{refresh_date}' as TIMESTAMP) as dss_create_at,
    C_UpdateDate                        as clz_update_date,
    C_Notes                             as clz_notes_original, 
    C_ProjectStatusIndicatorType        as clz_project_status_indicator_type
from 
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_projectstatsindicator PSI
join _clz_notes cln on psi.id = cln.id
left join 
    {var_client_custom_db}.datavault_dbo_s_project_details SPD
    on PSI.C_ProjectStatusIndicator = SPD.project 
where
    PSI.is_deleted = FALSE
    
""")

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_clz_service_type
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_r_clz_service_type AS
select 
  C_NatureofServiceCode as clz_nature_of_service_code,
  ExternalID as external_id, 
  SYSID as sys_id,
  id, 
  Name as name, 
  Description as description, 
  EntityType as entity_type,
  EntityOwner as entity_owner,
  ImageUrl as image_url,
  EmailsCount as emails_count, 
  cast(CurrencyExchangeDate as timestamp) as currency_exchange_at,
  CurrencyExchangeDateManuallySet as currency_exchange_date_manually_set,
  CreatedBy as created_by, 
  cast(CreatedOn as timestamp) as created_at, 
  LastUpdatedBy as last_updated_by,
  cast(LastUpdatedOn as timestamp) as last_updated_on_at,
  CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
  'SERVICE TYPE' as dss_record_source,
  CAST('{refresh_date}' as TIMESTAMP) as dss_create_at
from 
  (select 
    id, 
    CreatedBy, 
    CreatedOn, 
    LastUpdatedBy, 
    LastUpdatedOn, 
    EntityType, 
    Name, 
    Description, 
    ExternalID, 
    SYSID, 
    ImageUrl, 
    EntityOwner, 
    CurrencyExchangeDate, 
    EmailsCount, 
    CurrencyExchangeDateManuallySet, 
    C_NatureofServiceCode,
    is_deleted,
    row_number() over (partition by id order by lastupdatedon desc) as rn
    from  
    {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_servicetype
    ) CER 
where 
  CER.rn = 1 and 
  CER.is_deleted = false
order by 
  id, 
  CreatedBy,
  CreatedOn, 
  LastUpdatedBy, 
  LastUpdatedOn, 
  EntityType, 
  Name, 
  Description, 
  ExternalID, 
  SYSID, 
  ImageUrl, 
  EntityOwner, 
  CurrencyExchangeDate, 
  EmailsCount, 
  CurrencyExchangeDateManuallySet, 
  C_NatureofServiceCode
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_clz_JLL_Project_Types
spark.sql(""" 
CREATE OR REPLACE TABLE
	{var_client_custom_db}.datavault_dbo_r_clz_JLL_Project_Types  AS
select 
	id,
	ExternalID as external_id,
	SYSID as sys_id,
	Name as name,
	Description as description,
	EntityType as entity_type,
	EntityOwner as entity_owner,
	C_ProjectTypeE1Code as clz_project_type_e1_code,
    C_Region as clz_region,
    EmailsCount as emails_count,
	ImageUrl as image_url,
	cast(CurrencyExchangeDate as timestamp) as currency_exchange_date_at,
	CurrencyExchangeDateManuallySet as currency_exchange_date_manually_set,
	CreatedBy as created_by,
	cast(CreatedOn as timestamp) as created_at,
	LastUpdatedBy as last_updated_by,
	cast(LastUpdatedOn as timestamp) as last_updated_on_at,
	'PROJECT TYPE' as dss_record_source,
	CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
	CAST('{refresh_date}' as TIMESTAMP) as dss_create_at
from 
	(select 
		id, 
		CreatedBy, 
		CreatedOn, 
		LastUpdatedBy, 
		LastUpdatedOn, 
		EntityType, 
		Name, 
		Description, 
		ExternalID, 
		SYSID, 
		ImageUrl, 
		EntityOwner, 
		CurrencyExchangeDate, 
		EmailsCount, 
		CurrencyExchangeDateManuallySet, 
		C_ProjectTypeE1Code, 
        --Null as C_ProjectTypeE1Code,
		C_Region,
        --Null as C_Region,
		is_deleted,
		row_number() over (partition by id order by lastupdatedon desc) as rn
	from  
		{var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_jllprojecttype ) CER 
where 
	CER.rn = 1 and 
	CER.is_deleted = FALSE
order by   
	id, 
	CreatedBy, 
	CreatedOn, 
	LastUpdatedBy, 
	LastUpdatedOn, 
	EntityType, 
	Name, 
	Description, 
	ExternalID, 
	SYSID, 
	ImageUrl, 
	EntityOwner, 
	CurrencyExchangeDate, 
	EmailsCount, 
	CurrencyExchangeDateManuallySet, 
	C_ProjectTypeE1Code, 
	C_Region
 """.format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

# DBTITLE 1,datavault_dbo_r_clz_custom_value
spark.sql(""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_r_clz_custom_value AS
SELECT 
  id, 
  CreatedBy as created_by, 
  cast(CreatedOn as timestamp) as create_at, 
  LastUpdatedBy as last_updated_by, 
  EntityType as entity_type, 
  Name as name, 
  Description as description, 
  ExternalID as external_id, 
  SYSID as sysid, 
  ImageUrl as image_url, 
  EntityOwner as entity_owner, 
  cast(CurrencyExchangeDate as timestamp) as currency_exchange_date, 
  EmailsCount as emails_count, 
  CurrencyExchangeDateManuallySet as currency_exchange_date_manually_set, 
  C_Code as clz_code,
  C_FieldName as clz_field_name,
  C_CustomValueKey as clz_custom_value_key,
  C_AttributeWeightedScore as clz_attribute_weighted_score,
  C_Language as clz_language, 
  C_Customer as clz_customer,
  C_AttributeScore  clz_attribute_score,
  'CUSTOM VALUE' as dss_record_source,
  CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
  CAST('{refresh_date}' as TIMESTAMP) as dss_start_at,
  CAST('{refresh_date}' as TIMESTAMP) as dss_create_at,
  C_RPField as clz_rpfield,
  C_CustomValueKey2 as clz_custom_value_key2,
  C_FindObjectKey as clz_find_object_key
from 
  (SELECT 
      id, 
      CreatedBy, 
      CreatedOn, 
      LastUpdatedBy, 
      EntityType,
      Name, 
      Description, 
      ExternalID, 
      SYSID, 
      ImageUrl, 
      EntityOwner, 
      CurrencyExchangeDate, 
      EmailsCount, 
      CurrencyExchangeDateManuallySet,
      C_Customer,
      C_CustomValueKey2,
      C_FindObjectKey,
      C_RPField, 
      C_CustomValueKey, 
      C_Language,  
      C_AttributeScore ,
      C_AttributeWeightedScore,
      C_FieldName,
      C_Code
      ,is_deleted,
      row_number() over (partition by id order by lastupdatedon desc
      ) as rn
    FROM
        {var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_customvalue ) CER 
    where 
        CER.rn = 1 and CER.is_deleted = false
    order by 
        id, 
        CreatedBy, 
        CreatedOn, 
        LastUpdatedBy, 
        EntityType, 
        Name, 
        Description, 
        ExternalID, 
        SYSID, 
        ImageUrl, 
        EntityOwner, 
        CurrencyExchangeDate, 
        EmailsCount, 
        CurrencyExchangeDateManuallySet,
        C_Customer,
        C_CustomValueKey2,
        C_FindObjectKey,
        C_RPField, 
        C_CustomValueKey, 
        C_Language,  
        C_AttributeScore,
        C_AttributeWeightedScore, 
        C_FieldName,
        C_Code
""".format(var_client_custom_db=var_client_custom_db, var_azara_raw_db=var_azara_raw_db, var_client_name=var_client_name, refresh_date=refresh_date))

# COMMAND ----------

spark.sql(f""" 
CREATE OR REPLACE TABLE
{var_client_custom_db}.datavault_dbo_s_work_item_task_details AS
select
	DISTINCT
	Project,	--(hash Key replacement)
 	SYS_ID,	 	--(hash Key replacement)
	client_id,	--(hash Key replacement)
	c_customer_task,
	name,
	parent_project,
	parent,
	budget_status,
	likes_count,
	posts_count,
	c_prod,
	c_dev,
	c_uat,
	phase,
	last_updated_by,
	last_updated_on,
	created_by,
	created_on,
	description,
	c_service_product_e1_code,
	c_program_type,
	c_scope_status,
	c_milestone_number,
	c_risk_registry_score,
	c_risk_register_status,
	c_reporting_name,
	c_reporting_id,
	c_schedule_status,
	c_vendor,
	c_runtime_id,
	c_internal_type,
	c_project_building_size,
	c_service_product,
	clz_action_items_for,
	clz_project_program_for_meeting_minutes,
	clz_sprint_status,
	clz_comments,
	clz_cost_code,
	clz_cost_code_name,
	clz_cost_code_nlr,
	clz_task_status,
	clz_project_for_cost_code_item,
	clz_transfer_cost_code_item,
	clz_source_action_item,
	state,
	milestone,
	task_type,
	c_ajd30,
	c_ajd29,
	c_ajd28,
	c_ajd26,
	c_ajd21,
	c_ajd27,
	c_ajd25,
	c_ajd24,
	c_ajd23,
	c_ajd22,
	c_ajd20,
	c_ajd19,
	c_ajd18,
	c_ajd16,
	c_ajd17,
	c_ajd15,
	c_ajd14,
	c_ajd13,
	c_ajd12,
	c_ajd11,
	c_ajd09,
	c_ajd10,
	c_ajd08,
	c_ajd07,
	c_ajd06,
	c_ajd05,
	c_ajd04,
	c_ajd03,
	c_ajd02,
	c_ajd01,
	c_rp30,
	c_rp29,
	c_rp28,
	c_rp27,
	c_rp26,
	c_rp23,
	c_rp13,
	c_rp12,
	c_rp10,
	c_rp09,
	c_rp08,
	dss_record_source,
	dss_load_at,
	dss_start_at,
	dss_create_at,
	c_copied,
	last_updated_by_system_on_at,
	C_Package_Type,
	c_cost_code,
	c_cost_code_name,
	po_number,
	c_description_rt,
	c_responsible_party,
	c_minute_category,
	order_id,
	importance
FROM (
	select
		HW.client_id,
		(case C_Customertask 
		WHEN True then 1
		WHEN False then 0
		else C_Customertask
		END)  as c_customer_task,
		Name as name,
		ParentProject as parent_project,
		Parent as parent,
		BudgetStatus as budget_status,
		LikesCount as likes_count,
		PostsCount as posts_count,
		ifnull(GT.Project,'ABC') as Project,
		ifnull(GT.SYSID,'ABC') as SYS_ID,
		'' as c_prod,-- (not found)
		'' as c_dev, -- (not found)
		'' as c_uat, -- (not found)
		Phase as phase,
		LastUpdatedBy as last_updated_by,
		LastUpdatedOn as last_updated_on,
		CreatedBy as created_by,
		CreatedOn as created_on,
		Description as description,
		C_ServiceProductE1Code as c_service_product_e1_code,
		C_ProgramType as c_program_type,
		C_ScopeStatus as c_scope_status,
		C_MilestoneNumber as c_milestone_number,
		C_RiskRegistryScore as c_risk_registry_score,
		C_RiskRegisterStatus as c_risk_register_status,
		C_ReportingName as c_reporting_name,
		C_ReportingId as c_reporting_id,
		C_ScheduleStatus as c_schedule_status,
		C_Vendor as c_vendor,
		C_RuntimeId as c_runtime_id,
		C_InternalType as c_internal_type,
		C_ProjectBuildingSize as c_project_building_size,
		C_ServiceProduct as c_service_product,
		C_ActionItemsfor as clz_action_items_for,
		C_ProjectProgramforMeetingMinutes as clz_project_program_for_meeting_minutes,
--		C_SprintStatus as clz_sprint_status, (not available from source)
		'' as clz_sprint_status,
		C_Comments as clz_comments,
		C_CostCode as clz_cost_code,
		C_CostCodeName as clz_cost_code_name,
		C_CostCodeNLR as clz_cost_code_nlr,
		C_TaskStatus as clz_task_status,
		C_ProjectforCostCodeItem as clz_project_for_cost_code_item,
		C_TransferCostCodeItem as clz_transfer_cost_code_item,
		C_Source_ActionItem as clz_source_action_item,
		State as state,
		Milestone as milestone,
		TaskType as task_type,
		C_AJD30 as c_ajd30,
		C_AJD29 as c_ajd29,
		C_AJD28 as c_ajd28,
		C_AJD26 as c_ajd26,
		C_AJD21 as c_ajd21,
		C_AJD27 as c_ajd27,
		C_AJD25 as c_ajd25,
		C_AJD24 as c_ajd24,
		C_AJD23 as c_ajd23,
		C_AJD22 as c_ajd22,
		C_AJD20 as c_ajd20,
		C_AJD19 as c_ajd19,
		C_AJD18 as c_ajd18,
		C_AJD17 as c_ajd16,
		C_AJD16 as c_ajd17,
		C_AJD15 as c_ajd15,
		C_AJD14 as c_ajd14,
		C_AJD13 as c_ajd13,
		C_AJD12 as c_ajd12,
		C_AJD11 as c_ajd11,
		C_AJD09 as c_ajd09,
		C_AJD10 as c_ajd10,
		C_AJD08 as c_ajd08,
		C_AJD07 as c_ajd07,
		C_AJD06 as c_ajd06,
		C_AJD05 as c_ajd05,
		C_AJD04 as c_ajd04,
		C_AJD03 as c_ajd03,
		C_AJD02 as c_ajd02,
		C_AJD01 as c_ajd01,
		C_RP30 as c_rp30,
		C_RP29 as c_rp29,
		C_RP28 as c_rp28,
		C_RP27 as c_rp27,
		C_RP26 as c_rp26,
		C_RP23 as c_rp23,
		C_RP13 as c_rp13,
		C_RP12 as c_rp12,
		C_RP10 as c_rp10,
		C_RP09 as c_rp09,
		C_RP08 as c_rp08,
		'TASK' as dss_record_source,
		CAST('{refresh_date}' as TIMESTAMP) as dss_load_at,
		CAST('{refresh_date}' as TIMESTAMP) as dss_start_at,
		CAST('{refresh_date}' as TIMESTAMP) as dss_create_at,
	--	 as dss_version,
		(case C_Copied 
		WHEN True then 1
		WHEN False then 0
		else C_Copied
		END) as c_copied,
		LastUpdatedBySystemOn as last_updated_by_system_on_at,
	--	 as dss_current_version,
		C_PackageType as C_Package_Type,
		C_CostCode as c_cost_code,
		C_CostCodeName as c_cost_code_name,
		C_PONumber as po_number,
		C_DescriptionRT as c_description_rt,
		C_ResponsibleParty as c_responsible_party,
		C_MinuteCategory as c_minute_category,
		OrderID as order_id,
		Importance as importance
	from 
		{var_client_custom_db}.{var_client_name}_raw_edp_snapshot_clarizen_generictask GT
	LEFT JOIN
		(select 
			distinct 
			WI.Project as Project, 
			WI.SYSID as SYS_ID,
			'WORKITEM' as record_source,
			rc.client_id as client_id
		FROM
			{var_client_custom_db}.stage_clarizen_dbo_workitem WI
		left join 
			(select * from {var_client_custom_db}.datavault_dbo_r_client_mapping where UPPER(dss_record_source) = 'PROJECT') rc
			on rc.id = WI.Project
		order by 
			Project, SYS_ID) hw
		ON  ifnull(GT.project,'ABC') = ifnull(hw.project,'ABC') and ifnull(gt.sysid,'ABC') = ifnull(hw.sys_id,'ABC')  
	where
		is_deleted = FALSE )
""")