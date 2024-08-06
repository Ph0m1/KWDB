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


#ifndef INFOINDEX_H_
#define INFOINDEX_H_

#include <algorithm>
#include <iostream>
#include <vector>


template <typename T>
struct ObjectIndex
{
	T *obj;										///< pointer to the attributeInfo
	int index;									///< index of the dimension | offset in tag struct.
};

template <typename T, typename Compare>
struct ObjectCompare
{
	Compare comp_;
	bool operator() (const ObjectIndex<T> &l, const ObjectIndex<T> &r) {
		return comp_(*(l.obj), *(r.obj));
	}
};

template <typename T, typename Compare>
struct ObjectStringCompare
{
	Compare comp_;
	int operator() (const ObjectIndex<T> &l, const string &r) {
		return comp_(*(l.obj), r);
	}
};


template<class InputIterator, class T, typename Compare>
InputIterator binarySearch (InputIterator first, InputIterator last, const T &value, Compare comp)
{
	InputIterator initial_end = last;
	while (first < last) {
		InputIterator mid = first + (last - first - 1)/2;
		int res	= comp(*mid, value);
		if (res < 0)
	           first = mid + 1;
	       else if (res > 0)
	           last = mid;
	       else {
	    	   return mid;
	       }
	}
	return initial_end;
}


template <typename T, typename InfoCompare, typename StringCompare>
class InfoIndex
{
protected:
	std::vector<ObjectIndex<T> > index_;

public:
	InfoIndex() {};

	void setInfo(vector<T> &info);

	void clear() { index_.clear(); }

	T * getInfo(const string &name) const;

	int getIndex(const string &name) const;

	void print();
};

template<typename T, typename InfoCompare, typename StringCompare>
void InfoIndex<T, InfoCompare, StringCompare>::setInfo(vector<T> &info)
{
	for (unsigned int i = 0; i < info.size(); i++) {
		ObjectIndex<T> idx;
		idx.obj = &(info[i]);
		idx.index = i;
		index_.push_back(idx);
	}
	std::sort(index_.begin(), index_.end(), ObjectCompare<T, InfoCompare>());
}


template<typename T, typename InfoCompare, typename StringCompare>
T * InfoIndex<T, InfoCompare, StringCompare>::getInfo(const string &name) const
{
	typename std::vector<ObjectIndex<T> >::const_iterator it =
		binarySearch(index_.begin(), index_.end(), name, ObjectStringCompare<T, StringCompare>());

	if (it != index_.end())
		return (*it).obj;
	return nullptr;
}

template<typename T, typename InfoCompare, typename StringCompare>
int InfoIndex<T, InfoCompare, StringCompare>::getIndex(const string &name) const
{
	typename std::vector<ObjectIndex<T> >::const_iterator it =
		binarySearch(index_.begin(), index_.end(), name, ObjectStringCompare<T, StringCompare>());

	if (it != index_.end())
		return (*it).index;
	return -1;
}



template<typename T, typename InfoCompare, typename StringCompare>
void InfoIndex<T, InfoCompare, StringCompare>::print()
{
	for (unsigned int i = 0; i < index_.size(); i++) {
		std::cout << index_[i].obj->name << " " << index_[i].index << endl;
	}
}


#endif /* INFOINDEX_H_ */
