defmodule Signal.Aggregate do

    defprotocol Config do

        @fallback_to_any true
        @spec config(t) :: Keyword.t()
        def config(type)

    end

    defimpl Config, for: Any do
        def config(_type) do
            [timeout: Signal.Timer.seconds(5)]
        end
    end

    defmacro __using__(opts) do
        timeout = Keyword.get(opts, :timeout, Signal.Timer.seconds(5))
        quote do
            use Blueprint.Struct
            @signal__aggregate__timeout unquote(timeout)
            @before_compile unquote(__MODULE__)
        end
    end

    defmacro __before_compile__(_env) do

        quote generated: true, location: :keep do

            with timeout <- @signal__aggregate__timeout do
                defimpl Signal.Aggregate.Config do
                    @signal__config__timeout timeout
                    def config(_) do
                        [timeout: @signal__config__timeout]
                    end
                end
            end

            defimpl Signal.Stream.Reducer do
                def apply(aggr, event) do 
                    # Check if the event has a custom reduction
                    # function and use that else reduce the
                    # event with the aggregate reduce func
                    case Signal.Stream.Reducer.impl_for(event) do
                        nil ->
                            %{__struct__: emod} = event
                            %{__struct__: amod} = aggr
                            etype = inspect(emod)
                            atype = inspect(amod)
                            raise """ 
                            #{atype} can not reduce event #{etype}

                            No stream reducer implemented for %#{etype}{...}

                            Options

                            - 1) using Signal.Event with an apply/2 callback

                                defmodule #{etype} do
                                    use Signal.Event,
                                        stream: {#{atype}, ...}
                                    
                                    def apply(%#{etype}{}, %#{atype}{}) do
                                        ....
                                    end
                                end

                            - 2) define a stream reducer protocol for **ALL AGGREGATE STREAM EVENTS**
                                defmodule #{atype} do
                                    defstruct [...]

                                    defimple Signal.Stream.Reducer do
                                        def apply(%#{etype}{}, %#{atype}{}) do
                                            ....
                                        end
                                    end
                                end
                                
                            Note the second option (2) will intercept ALL events
                            regardless of if the event was defined with `use Signal.Event`

                            """
                        _impl ->
                            Signal.Stream.Reducer.apply(event, aggr)
                    end
                end
            end
        end
    end

    def snapshot(aggregate, action \\  nil)
    def snapshot(aggregate, nil) do
        {:snapshot, aggregate}
    end
    def snapshot(aggregate, :sleep) do
        {:snapshot, aggregate, :sleep}
    end
    def snapshot(aggregate, [timeout: timeout]) when is_number(timeout) do
        {:snapshot, aggregate, timeout}
    end

    def sleep(aggregate, action \\  nil)
    def sleep(aggregate, nil) do
        {:sleep, aggregate}
    end
    def sleep(aggregate, [timeout: timeout]) when is_number(timeout) do
        {:sleep, aggregate, timeout}
    end

    def continue(aggregate, action \\  nil)
    def continue(aggregate, nil) do
        {:ok, aggregate}
    end
    def continue(aggregate, [timeout: timeout]) when is_number(timeout) do
        {:ok, aggregate, timeout}
    end

end

