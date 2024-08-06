// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.


#include "legacy.h"

/*
void formUniformBigTable(std::vector<std::string> & measure_name_, const string &tbl_sub_path)
{

	int uniform_BT_index = 0;
	string major_bt_url = measure_name_[uniform_BT_index];
	std::cout << "\n=================formUniformBigTable : " << major_bt_url << " =====================";


	BigTable *bt_major = getBigTable(major_bt_url);
	NameService *ns = getDefaultNameService();
	int64_t record_of_bt_major = bt_major->size();
	int bt_major_column_number = bt_major->numKeyColumn();

	std::vector<ExtendOrder> sort_order;
	for(int s_o = 0 ; s_o < bt_major_column_number ; s_o++)
	{
		ExtendOrder col_order;
		col_order.index = s_o;
		sort_order.push_back(std::move(col_order));
//		sort_order.push_back(s_o);
	}

	//TODO sort
//	OrderedBigTable obt_major(bt_major);
	ExtendedBigTable obt_major(bt_major);
	Behavior behavior;
	behavior.sort(obt_major, sort_order);
///	bt_major->sort(sort_order);

	for(int i = 1; i < (int)measure_name_.size(); i++)
	{

//		bt_major->sort(sort_order);
//		DataType data_type;

		string mmbt_url;
//		mmbt_url = "mmbt:///";
		mmbt_url += measure_name_[i];
//		mmbt_url += ".bt";

		BigTable *bt = getBigTable(mmbt_url);
		int64_t record_of_bt = bt->size();

///		bt->sort(sort_order);
		int bt_column_number = bt->numKeyColumn();

		std::string temp_data_bt_url = "mmbt:///";
		int name_start_pos = measure_name_[uniform_BT_index].find("mmbt://");
		name_start_pos += 8;
		std::string measure_name_only = measure_name_[uniform_BT_index].substr(name_start_pos ,  measure_name_[uniform_BT_index].length() - name_start_pos);
		temp_data_bt_url += genTempObjectURL(measure_name_only);

//		temp_data_bt_url += genTempObjectURL(measure_name_[uniform_BT_index]);
		temp_data_bt_url += ".bt";
		BigTable *temp_data_bt = getBigTable(temp_data_bt_url, tbl_sub_path, O_CREAT|O_TRUNC);
//		std::vector<string> attrs = bt_major->hierarchyAttribute();
		std::vector<AttributeInfo> attrs;
		attrs = *(bt_major->getHierarchyInfo());
//		bt_major->getHierarchyAttributesSeries(attrs);

		DataType data_type_major;
		bt_major->getDataType(data_type_major);

		temp_data_bt->create(temp_data_bt_url, attrs, data_type_major);

		int index_of_bt_major = 0;
		int index_of_bt = 0;

		IDTYPE * bt_major_row;
		IDTYPE * bt_row;

		do
		{
			bt_major_row = bt_major->record(index_of_bt_major);
			bt_row = bt->record(index_of_bt);
			int check_the_same = 0;

			// check whether the record on the top is the same or not
			for(int c_sim = 0 ; c_sim < (int)sort_order.size() ; c_sim++)
			{
				int index_of_unique_key = c_sim;
				if(index_of_unique_key < bt_major_column_number && index_of_unique_key < bt_column_number
				&& bt_major_row[index_of_unique_key] != bt_row[index_of_unique_key])
				{
					if(bt_major_row[index_of_unique_key] > bt_row[index_of_unique_key])
					{
						check_the_same = 1;
					}
					else
					{
						check_the_same = -1;
					}
					break;
				}

			}

			// two element is the same
			if(check_the_same == 0)
			{
				index_of_bt_major++;
				index_of_bt++;
//				std::cout << "matched" << std::endl;
			}
			// bt_major_row element > bt_row element
			else if(check_the_same == 1)
			{
				// insert bt_row into major_bt
				std::vector<int64_t> temp_row;
//				std::cout << "push row => ";
				for(int c_n = 0 ; c_n < bt_column_number ; c_n++)
				{
					temp_row.push_back(bt_row[c_n]);
//					std::cout << ns->idToString(bt_row[c_n])<< " ";
				}
				DataType temp_data_type;
				bt_major->getDataType(temp_data_type);
				for(int d_n = 0 ; d_n < temp_data_type.num_series ; d_n++)
				{
					temp_row.push_back(0);
//					std::cout << "0 ";
				}

//				std::cout << std::endl;
				///TODO: update push_back
				// temp_data_bt->push_back(temp_row);
				index_of_bt++;

			}
			// bt_major_row element < bt_row element
			else if(check_the_same == -1)
			{
				index_of_bt_major++;
//				std::cout << "check in major" << std::endl;
			}
		}while(index_of_bt_major < record_of_bt_major && index_of_bt < record_of_bt);


		if(index_of_bt != record_of_bt)
		{
			for(int insert_p = index_of_bt ; insert_p < record_of_bt ; insert_p++)
			{
				bt_row = bt->record(insert_p);
				std::vector<int64_t> temp_row;
//				std::cout << "R push row => ";
				for(int c_n = 0 ; c_n < bt_column_number ; c_n++)
				{
					temp_row.push_back(bt_row[c_n]);
//					std::cout << ns->idToString(bt_row[c_n])<< " ";
				}
				DataType temp_data_type;
				bt_major->getDataType(temp_data_type);
				for(int d_n = 0 ; d_n < temp_data_type.num_series ; d_n++)
				{
					temp_row.push_back(0);
//					std::cout << "0 ";
				}

//				std::cout << std::endl;
				///TODO: update push_back
				// temp_data_bt->push_back(temp_row);

			}
		}

		////////////////
		//
		if(temp_data_bt->size() > 0)
		{
			std::cout << " ============temp_data_bt print=========== " << std::endl;
			DataType temp_data_type;
			bt_major->getDataType(temp_data_type);
			int bt_major_total_length = bt_major_column_number + temp_data_type.num_series;

			for(int insert_m = 0 ; insert_m < (int)temp_data_bt->size() ; insert_m++)
			{

//				std::cout << "push temp to major ";
				std::vector<int64_t> temp_row_t;
				IDTYPE * temp_data_bt_record = temp_data_bt->record(insert_m);
				for(int t_d = 0 ; t_d < bt_major_total_length ; t_d++)
				{


					if(t_d < bt_major_column_number)
					{
						temp_row_t.push_back(temp_data_bt_record[t_d]);
//						std::cout << ns->idToString(temp_data_bt_record[t_d]) + " ";
					}
					else
					{

						temp_row_t.push_back(0);

					}


				}
//				std::cout << std::endl;
				///TODO: update push_back
				// bt_major->push_back(temp_row_t);
			}
		}
		record_of_bt_major = bt_major->size();

		releaseObject(mmbt_url);
		releaseObject(temp_data_bt_url);
	}

//	std::cout <<  " ---------------------------------------------------=" << std::endl;
//
//	bt_major->sort(sort_order);
//	bt_major->print(std::cout);
//	std::cout <<  " ---------------------------------------------------=" << std::endl;

	// release BigObject, BigTable, NameService
	releaseDefaultNameService();
	releaseObject(major_bt_url);
	////////////////////////////////////////////////////////////////////////

}

void extendBTByUniformBT(std::vector<std::string> & measure_name_ ,
	int target_BT_index, const string &tbl_sub_path)
{
	int uniform_BT_index = 0;

//	std::cout << "\n=================extend BigTables by the Uniform BigTable======================" << std::endl;
	string major_bt_url = measure_name_[target_BT_index];

	std::cout << "\n================= extend ";
	std::cout << major_bt_url;
	std::cout << " by the Uniform BigTable ======================" << std::endl;


	BigTable *bt_major = getBigTable(major_bt_url);
//	NameService *ns = getDefaultNameService();

//	std::vector<int> unique_key_index;
	std::vector<ExtendOrder> unique_key_index;
	for(int i = 0 ; i < bt_major->numKeyColumn() ; i++)
	{
		ExtendOrder col_order;
		col_order.index = i;
		unique_key_index.push_back(std::move(col_order));
//		unique_key_index.push_back(i);
	}

	int64_t record_of_bt_major = bt_major->size();
	///
///	bt_major->sort(unique_key_index);
	int bt_major_column_number = bt_major->numKeyColumn();




	vector<int> sort_order;
//	DataType data_type;

	string mmbt_url;
//	mmbt_url = "mmbt:///";
	mmbt_url += measure_name_[uniform_BT_index];
//	mmbt_url += ".bt";

	BigTable *bt = getBigTable(mmbt_url);
	int64_t record_of_bt = bt->size();
	////
///	bt->sort(unique_key_index);
	int bt_column_number = bt->numKeyColumn();


	string temp_data_bt_url = "mmbt:///";
	int name_start_pos = measure_name_[uniform_BT_index].find("mmbt://");
	name_start_pos += 8;
	std::string measure_name_only = measure_name_[uniform_BT_index].substr(name_start_pos ,  measure_name_[uniform_BT_index].length() - name_start_pos);
	temp_data_bt_url += genTempObjectURL(measure_name_only);
//	temp_data_bt_url += genTempObjectURL(measure_name_[target_BT_index]);
	temp_data_bt_url += ".bt";


	BigTable *temp_data_bt = getBigTable(temp_data_bt_url , tbl_sub_path,
		O_CREAT|O_TRUNC);
//	std::vector<string> attrs = bt_major->hierarchyAttribute();
	std::vector<AttributeInfo> attrs;
	attrs = *(bt_major->getHierarchyInfo());
//	bt_major->getHierarchyAttributesSeries(attrs);


	DataType data_type_major;
	bt_major->getDataType(data_type_major);

	temp_data_bt->create(temp_data_bt_url, attrs, data_type_major);

	int index_of_bt_major = 0;
	int index_of_bt = 0;

//		int check_process = 0;

	IDTYPE * bt_major_row;
	IDTYPE * bt_row;
	do
	{
		bt_major_row = bt_major->record(index_of_bt_major);
		bt_row = bt->record(index_of_bt);
		int check_the_same = 0;

		// check whether the record on the top is the same or not
		for(int c_sim = 0 ; c_sim < (int)unique_key_index.size() ; c_sim++)
		{
//			int index_of_unique_key = unique_key_index[c_sim];
			int index_of_unique_key = unique_key_index[c_sim].index;
			if(index_of_unique_key < bt_major_column_number && index_of_unique_key < bt_column_number
			&& bt_major_row[index_of_unique_key] != bt_row[index_of_unique_key])
			{
				if(bt_major_row[index_of_unique_key] > bt_row[index_of_unique_key])
				{
					check_the_same = 1;
				}
				else
				{
					check_the_same = -1;
				}
				break;
			}
		}

		// two element is the same
		if(check_the_same == 0)
		{
			index_of_bt_major++;
			index_of_bt++;
		}
		// bt_major_row element > bt_row element
		else if(check_the_same == 1)
		{
			// insert bt_row into major_bt
			std::vector<int64_t> temp_row;
			for(int c_n = 0 ; c_n < bt_column_number ; c_n++)
			{
				temp_row.push_back(bt_row[c_n]);
			}
			DataType temp_data_type;
			bt_major->getDataType(temp_data_type);
			for(int d_n = 0 ; d_n < temp_data_type.num_series ; d_n++)
			{
				temp_row.push_back(0);
			}

			///TODO: update push_back
			// temp_data_bt->push_back(temp_row);
			index_of_bt++;

		}
		// bt_major_row element < bt_row element
		else if(check_the_same == -1)
		{
			index_of_bt_major++;
		}
	}while(index_of_bt_major < record_of_bt_major && index_of_bt < record_of_bt);


	if(index_of_bt != record_of_bt)
	{
		for(int insert_p = index_of_bt ; insert_p < record_of_bt ; insert_p++)
		{
			bt_row = bt->record(insert_p);
			std::vector<int64_t> temp_row;
			for(int c_n = 0 ; c_n < bt_column_number ; c_n++)
			{
				temp_row.push_back(bt_row[c_n]);
			}
			DataType temp_data_type;
			bt_major->getDataType(temp_data_type);
			for(int d_n = 0 ; d_n < temp_data_type.num_series ; d_n++)
			{
				temp_row.push_back(0);
			}
			///TODO: update push_back
			//temp_data_bt->push_back(temp_row);

		}
	}

	////////////////
	//
	if(temp_data_bt->size() > 0)
	{

		DataType temp_data_type;
		bt_major->getDataType(temp_data_type);
		int bt_major_total_length = bt_major_column_number + temp_data_type.num_series;

		for(int insert_m = 0 ; insert_m < (int)temp_data_bt->size() ; insert_m++)
		{
			std::vector<int64_t> temp_row_t;
			IDTYPE * temp_data_bt_record = temp_data_bt->record(insert_m);
			for(int t_d = 0 ; t_d < bt_major_total_length ; t_d++)
			{
				if(t_d < bt_major_column_number)
				{
					temp_row_t.push_back(temp_data_bt_record[t_d]);
				}
				else
				{

					temp_row_t.push_back(0);

				}


			}
			///TODO: update push_back
			// bt_major->push_back(temp_row_t);
		}

	}

	releaseObject(mmbt_url);
	releaseObject(temp_data_bt_url);


	std::cout << "\n================= extend ";
	std::cout << major_bt_url;
	std::cout << " by the Uniform BigTable done ======================" << std::endl;

//	std::cout <<  " ---------------------------------------------------=" << std::endl;
//	bt_major->print(std::cout);
//	std::cout <<  " ---------------------------------------------------=" << std::endl;

	// release BigObject, BigTable, NameService
	releaseDefaultNameService();
	releaseObject(major_bt_url);

}

int unifyMeasureSpace(std::vector<std::string> & measure_name,
	const string &tbl_sub_path)
{
	//TODO: check hierarchy number, if the number is different, return 0;
	if(measure_name.size() > 1)
	{
		formUniformBigTable(measure_name, tbl_sub_path);
		for(int index_mea = 1 ; index_mea < (int)measure_name.size() ; index_mea++)
		{
			extendBTByUniformBT(measure_name, index_mea, tbl_sub_path);
		}
	}

	return 1;
}

*/
