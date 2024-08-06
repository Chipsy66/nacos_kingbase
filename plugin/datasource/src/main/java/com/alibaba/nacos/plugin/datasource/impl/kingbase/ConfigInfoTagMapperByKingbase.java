package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoTagMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.Collections;

/**
 * The postgresql implementation of ConfigTagsRelationMapper.
 *
 * @author hyx
 * @author laokou
 **/

public class ConfigInfoTagMapperByKingbase extends AbstractMapperByKingbase implements ConfigInfoTagMapper {

	@Override
	public String getDataSource() {
		return DataSourceConstant.KINGBASE;
	}

	@Override
	public MapperResult findAllConfigInfoTagForDumpAllFetchRows(MapperContext context) {
		int startRow = context.getStartRow();
		int pageSize = context.getPageSize();
		String sql = " SELECT t.id,data_id,group_id,tenant_id,tag_id,app_name,content,md5,gmt_modified "
				+ " FROM (  SELECT id FROM config_info_tag  ORDER BY id LIMIT " + pageSize + " OFFSET " + startRow
				+ " ) " + "g, config_info_tag t  WHERE g.id = t.id  ";
		return new MapperResult(sql, Collections.emptyList());
	}

}
