package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.GroupCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

/**
 * The postgresql implementation of {@link GroupCapacityMapper}.
 *
 * @author lixiaoshuang
 * @author laokou
 */
public class GroupCapacityMapperByKingbase extends AbstractMapperByKingbase implements GroupCapacityMapper {

	@Override
	public String getDataSource() {
		return DataSourceConstant.KINGBASE;
	}

	@Override
	public MapperResult selectGroupInfoBySize(MapperContext context) {
		String sql = "SELECT id, group_id FROM group_capacity WHERE id > ? LIMIT ?";
		return new MapperResult(sql, CollectionUtils.list(context.getWhereParameter("id"), context.getPageSize()));
	}

}