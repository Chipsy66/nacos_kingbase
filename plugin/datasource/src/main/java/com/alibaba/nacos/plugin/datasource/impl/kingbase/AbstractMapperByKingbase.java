package com.alibaba.nacos.plugin.datasource.impl.kingbase;

import com.alibaba.nacos.plugin.datasource.enums.kingbase.TrustedKingbaseFunctionEnum;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;

/**
 * The postgresql implementation of ConfigTagsRelationMapper.
 *
 * @author hyx
 * @author laokou
 **/

public abstract class AbstractMapperByKingbase extends AbstractMapper{

	@Override
	public String getFunction(String functionName) {
		return TrustedKingbaseFunctionEnum.getFunctionByName(functionName);
	}

}
