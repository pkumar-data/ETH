--{{ audit_helper.compare_relations(source('eth', 'contracts'), source('eth', 'contracts_clone'))}}





{{codegen.generate_model_yaml(['stablecoin_activity_per_day'])}}