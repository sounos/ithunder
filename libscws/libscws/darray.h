/**
 * @file darray.h (double array)
 * @author Hightman Mar
 * @editor set number ; syntax on ; set autoindent ; set tabstop=4 (vim)
 * $Id: darray.h,v 1.2 2011/05/16 06:00:28 hightman Exp $
 */

#ifndef	_SCWS_DARRAY_20070525_H_
#define	_SCWS_DARRAY_20070525_H_

void **darray_new(int row, int col, int size);
void darray_free(void **arr);

#endif
