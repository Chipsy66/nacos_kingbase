package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.HistoryConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

/**
 * The postgresql implementation of HistoryConfigInfoMapper.
 *
 * @author hyx
 * @author laokou
 **/

public class HistoryConfigInfoMapperByKingbase extends AbstractMapperByKingbase implements HistoryConfigInfoMapper {

	@Override
	public String getDataSource() {
		return DataSourceConstant.KINGBASE;
	}

	@Override
	public MapperResult removeConfigHistory(MapperContext context) {
		String sql = "DELETE FROM his_config_info WHERE gmt_modified < ? LIMIT ?";
		return new MapperResult(sql,
				CollectionUtils.list(context.getWhereParameter("startTime"), context.getWhereParameter("limitSize")));
	}

	@Override
	public MapperResult pageFindConfigHistoryFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = "SELECT nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified FROM his_config_info "
				+ "WHERE data_id = ? AND group_id = ? AND tenant_id = ? ORDER BY nid DESC  LIMIT " + pageSize
				+ " OFFSET " + startRow;
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter("dataId"),
				context.getWhereParameter("groupId"), context.getWhereParameter("tenantId")));
	}

}