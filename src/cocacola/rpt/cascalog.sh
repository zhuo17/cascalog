dw_cascalog='/home/spiderdt/work/git/spiderdt-team/data-platform/job-launcher/dw_cascalog.sh'

set -e

 # echo "[running channel...]"
 # ${dw_cascalog} rpt.m-cocacola-score_channel
 # echo "[run channel done!]"
 # 
 # echo "[running channel_bg...]"
 # ${dw_cascalog}  rpt.m-cocacola-score_channel_bg
 # echo "[run channel_bg done!]"
 # 
 # echo "[running kpi...]"
 # ${dw_cascalog} rpt.m-cocacola-score_kpi
 # echo "[run kpi done!]"
 # 
 # echo "[running kpi_bg...]"
 # ${dw_cascalog} rpt.m-cocacola-score_kpi_bg
 # echo "[run kpi_bg done!]"
 # 
 #echo "[running overall...]"
 #${dw_cascalog} rpt.m-cocacola-score_overall
 #echo "[run overall done!]"
 #
 #echo "[running opportunity...]"
 #${dw_cascalog} rpt.m-cocacola-score_opportunity
 #echo "[run ooportinity done!]"
 #
 #echo "[running channel_metrics_opptunity...]"
 #${dw_cascalog} rpt.m-cocacola-score_channel_metrics_opportunity
 #echo "[run channel_metrics_opptunity done!]"
 #
 #echo "[running bottler_ranking...]"
 #${dw_cascalog} rpt.m-cocacola-score_bottler_ranking
 #echo "[run bottler_ranking done!]"
 
 echo "[running period...]"
 ${dw_cascalog} rpt.m-cocacola-score_period
 echo "[run period done!]"
 
 echo "[running period_months...]"
 ${dw_cascalog} rpt.m-cocacola-score_period_months
 echo "[run period_months done!]"
 
 echo "[running top_bottom5_ranking...]"
 ${dw_cascalog} rpt.m-cocacola-score_top_bottom5_ranking
 echo "[run top_bottom5_ranking done!]"
 
 echo "[running gt_rural...]"
 ${dw_cascalog} rpt.m-cocacola-score_gt_rural
 echo "[run gt_rural done!]"
 
 echo "[running gt_rural_bg...]"
 ${dw_cascalog} rpt.m-cocacola-score_gt_rural_bg
 echo "[run gt_rural_bg done!]"
 
 echo "[running gt_rural_period_bg]"
 ${dw_cascalog} rpt.m-cocacola-score_gt_rural_period_bg
 echo "[run gt_rural_period_bg done!]"
 
 echo "[running skus...]"
 ${dw_cascalog} rpt.m-cocacola-skus
 echo "[run skus done!]"
 
 echo "[running sku6_bg...]"
 ${dw_cascalog} rpt.m-cocacola-sku6_bg
 echo "[run sku6_bg done!]"
 
 echo "[running sku6_bottler...]"
 ${dw_cascalog} rpt.m-cocacola-sku6_bottler
 echo "[run sku6_bottler done!]"
 
 echo "[running sku6_period_bg...]"
 ${dw_cascalog} rpt.m-cocacola-sku6_period_bg
 echo "[run sku6_period_bg]"
 
 echo "[running availability_brand...]"
 ${dw_cascalog} rpt.m-cocacola-availability_brand
 echo "[run availability done!]"
 
 echo "[running availability_period_trend...]"
 ${dw_cascalog} rpt.m-cocacola-availability_period_trend
 echo "[run availability_period_trend done!]"
 
 echo "[running sovi_brand...]"
 ${dw_cascalog} rpt.m-cocacola-sovi_brand
 echo "[run sovi_brand done!]"
 
 echo "[running sovi_period_trend...]"
 ${dw_cascalog} rpt.m-cocacola-sovi_period_trend
 echo "[run sovi_period_trend done!]"
 
 echo "[running sovi_period_trend6...]"
 ${dw_cascalog} rpt.m-cocacola-sovi_period_trend6
 echo "[run sovi_period_trend6 done!]"
 
 echo "[running cooler_brand...]"
 ${dw_cascalog} rpt.m-cocacola-cooler_brand
 echo "[run cooler_brand done!]"

 echo "[running cooler_period_trend...]"
 ${dw_cascalog} rpt.m-cocacola-cooler_period_trend
 echo "[run cooler_period_trend done!]"
 
 echo "[running activation_period_trend...]"
 ${dw_cascalog} rpt.m-cocacola-activation_period_trend
 echo "[run activation_period_trend done!]"
 
 echo "[running activation_brand...]"
 ${dw_cascalog} rpt.m-cocacola-activation_brand
 echo "[run activation_brand done!]"

echo "[running activation_sub_brand...]"
${dw_cascalog} rpt.m-cocacola-activation_sub_brand
echo "[run activation_sub_brand done!]"

echo "[running availability_rural...]"
${dw_cascalog} rpt.m-cocacola-availability_rural
echo "[run availability_rural done!]" 

