local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-program,disjoin-slave,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))
