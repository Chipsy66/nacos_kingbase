package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.plugin.datasource.constants.DataSourceConstant;
import com.alibaba.nacos.plugin.datasource.mapper.TenantInfoMapper;

/**
 * The postgresql implementation of TenantInfoMapper.
 *
 * @author hyx
 * @author laokou
 **/

public class TenantInfoMapperByKingbase extends AbstractMapperByKingbase implements TenantInfoMapper {

	@Override
	public String getDataSource() {
		return DataSourceConstant.KINGBASE;
	}

}
