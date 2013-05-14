AC_DEFUN([AC_CHECK_EXTRA_OPTIONS],[
        AC_MSG_CHECKING(for debugging)
        AC_ARG_ENABLE(debug, [  --enable-debug  compile for debugging])
        if test -z "$enable_debug" ; then
                enable_debug="no"
        elif test $enable_debug = "yes" ; then
                CPPFLAGS="${CPPFLAGS} -g -D_DEBUG"
        fi
        AC_MSG_RESULT([$enable_debug])

        AC_MSG_CHECKING(for pthread_mutex)
        AC_ARG_ENABLE(pthread_mutex, [  --enable-pthread_mutex  compile for enable pthread mutex])
        if test -z "$enable_pthread_mutex" ; then
            enable_pthread_mutex="no"
        elif test $enable_pthread_mutex = "yes" ; then
            CPPFLAGS="${CPPFLAGS} -DUSE_PTHREAD_MUTEX"
        fi
        AC_MSG_RESULT([$enable_pthread_mutex])

        AC_MSG_CHECKING(for index_compress)
        AC_ARG_ENABLE(compress, [  --disable-compress    compile for disable index compress], [ disable_compress="yes"], [ disable_compress="no" ])
        if test "$disable_compress" = "yes" ; then
                CPPFLAGS="${CPPFLAGS} -D_DISABLE_COMPRESS"
        fi
        AC_MSG_RESULT([$enable_compress])

        AC_MSG_CHECKING(whether to disable memindex)
        AC_ARG_ENABLE(memindex, [  --disable-memindex   compile for disable memindex], [ disable_memindex="yes" ], [ disable_memindex="no" ])
        if test "$disable_memindex" = "yes" ; then
                CPPFLAGS="${CPPFLAGS} -D_DISABLE_MEMINDEX"
        fi
        AC_MSG_RESULT([$enable_memindex])
])

