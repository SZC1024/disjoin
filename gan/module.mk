local_src := $(wildcard $(subdirectory)/*.cpp)

$(eval $(call make-program,master,libtriplebit.a,$(local_src)))

$(eval $(call compile-rules))

