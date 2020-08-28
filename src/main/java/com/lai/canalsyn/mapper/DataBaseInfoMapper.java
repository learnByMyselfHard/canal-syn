package com.lai.canalsyn.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/23 13:39
 * @ Description :
 */
@Mapper
public interface DataBaseInfoMapper {

    @Select("show databases")
    public List<String>  getAllDataBases();


}
