select
    app_name,
    sum(tvs),
    sum(case when comb_entrypoint_flag = 1 then tvs end) as deeplink_me_tvs,
    sum(case when comb_entrypoint_flag = 2 then tvs end) as section_guide_me_tvs,
    sum(case when comb_entrypoint_flag = 3 then tvs end) as search_me_tvs,
    sum(case when comb_entrypoint_flag = 4 then tvs end) as linear_me_tvs
from
    SANDBOX.ANALYSIS_PRODUCT.VODENTRIES_NN_NEW_062223 
group by
    1