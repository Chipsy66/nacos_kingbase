package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.TableConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoAggrMapper;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.List;

/**
 * The mysql implementation of ConfigInfoAggrMapper.
 *
 * @author zc
 **/

public class ConfigInfoAggrMapperByKingbase extends AbstractMapperByKingbase implements ConfigInfoAggrMapper {


	@Override
	public MapperResult findConfigInfoAggrByPageFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String dataId = (String) context.getWhereParameter("dataId");
		String groupId = (String) context.getWhereParameter("groupId");
		String tenantId = (String) context.getWhereParameter("tenantId");
		String sql = "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM config_info_aggr WHERE data_id= ? AND "
				+ "group_id= ? AND tenant_id= ? ORDER BY datum_id LIMIT " + pageSize + " OFFSET " + startRow;
		List<Object> paramList = CollectionUtils.list(new Object[] { dataId, groupId, tenantId });
		return new MapperResult(sql, paramList);
	}

	@Override
	public String getTableName() {
		return TableConstant.CONFIG_INFO_AGGR;
	}

	@Override
	public String getDataSource() {
		return DataSourceConstant.KINGBASE;
	}

}