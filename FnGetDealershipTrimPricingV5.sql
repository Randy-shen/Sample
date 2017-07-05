CREATE FUNCTION [dlr].[FnGetDealershipTrimPricingV6]
  (
    @IsUnConfiguredFlag BIT = 1,
    @ProgramAffinityGroupId bigint,
    @SearchPostalCode varchar(5),
    @TrimId bigint,
    @MakeId bigint,
    @BestPriceQuote BIT = 0,
    @DefaultDealershipId BIGINT = null,
    @Distance INT = null,
    @ExteriorColorId BIGINT = NULL,
    @InteriorColorId BIGINT = NULL,
    @OptionIds VARBINARY(8000) = NULL,
    @InstalledOptions MONEY = NULL,
    @IncentiveIds VARBINARY(8000) = NULL,
    @InstalledIncentives MONEY = NULL,
    @IsLoggedIn BIT = 0,
    @IncludeProgramIncentives BIT = 1,
    @AbTests VARCHAR(4000) = NULL,
    @IncludeAllDealershipsForLogging BIT = 0,
    @QueryType VARCHAR(50) = 'Trim',
    @DealershipIds VARBINARY(8000) = NULL
  )
  RETURNS TABLE
AS
  RETURN
  (
    SELECT
      TrimId,
      DealershipId,

      StickerBase,
      StickerRegionalFees,
      StickerOptions,
        StickerTotal =
                     StickerBase + DestinationFee + StickerRegionalFees + StickerOptions,
        StickerNoOptionsTotal =
                              StickerBase + DestinationFee + StickerRegionalFees,

      IsInvoiceVisibleFlag,
      InvoiceBase,
      InvoiceOptions,
      InvoiceOptionsRegionalFees,
        InvoiceRegionalFees =
                            InvoiceRegionalFees + InvoiceOptionsRegionalFees,
        InvoiceTotal =
                     InvoiceBase + DestinationFee + InvoiceRegionalFees + InvoiceOptions + InvoiceOptionsRegionalFees,
        InvoiceNoOptionsTotal =
                              InvoiceBase + DestinationFee + InvoiceRegionalFees,

      MemberNetworkOffset,
      OpenNetworkPremium,

        OptionSavings =
                      StickerOptions
                      - (InvoiceOptions + InvoiceOptionsRegionalFees),

        PriceExperience =
                        CASE
                        WHEN
                          (PriceExperience = 'Public' OR (PriceExperience = 'Upfront' AND @IsLoggedIn = 0 AND @ProgramAffinityGroupId = 120))
                          AND IsOemExperienceFlag = 1
                          AND (InvoiceBase + DestinationFee + InvoiceRegionalFees + ConfiguratorTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive) IS NOT NULL -- ConfiguratorTotal is not null
                          AND (InvoiceBase + DestinationFee + InvoiceRegionalFees + ConfiguratorTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive)  -- ConfiguratorTotal < StickerTotal
                              < (StickerBase + DestinationFee + StickerRegionalFees + StickerOptions) THEN 'OEM'
                        ELSE PriceExperience
                        END,


      TotalOffset,

      UpfrontTotalOffset,


        Base =
             InvoiceBase + TotalOffset - DealershipIncentives,

        UpfrontBase =
                    InvoiceBase + UpfrontTotalOffset - DealershipIncentives,


        Total =
              InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive,

        UpfrontTotal =
                     InvoiceBase + DestinationFee + UpfrontInvoiceRegionalFees + UpfrontTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + UpfrontInvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive,

        TotalWithDealerFeesAndAccessories =
                                          InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive + DealershipDiscountedAccessoryTotal + DealershipFeeTotal + DocumentationFee,

        NoOptionsTotal =
                       InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive,

        UpfrontNoOptionsTotal =
                              InvoiceBase + DestinationFee + InvoiceRegionalFees + UpfrontTotalOffset - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive,


        TotalSavingsFromSticker =
                                (StickerBase + DestinationFee + StickerRegionalFees + StickerOptions)
                                - (InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive),

        UpfrontTotalSavingsFromSticker =
                                       (StickerBase + DestinationFee + StickerRegionalFees + StickerOptions)
                                       - (InvoiceBase + DestinationFee + InvoiceRegionalFees + UpfrontTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive),


        TotalSavingsFromStickerPercent =
                                       (
                                         ((StickerBase + DestinationFee + StickerRegionalFees + StickerOptions)
                                          - (InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive))
                                         / nullif((StickerBase + DestinationFee + StickerRegionalFees + StickerOptions), 0)
                                       ) * 100,

        UpfrontTotalSavingsFromStickerPercent =
                                              (
                                                ((StickerBase + DestinationFee + StickerRegionalFees + StickerOptions)
                                                 - (InvoiceBase + DestinationFee + InvoiceRegionalFees + UpfrontTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive))
                                                / nullif((StickerBase + DestinationFee + StickerRegionalFees + StickerOptions), 0)
                                              ) * 100,


        TotalSavingsFromInvoice =
                                (InvoiceBase + DestinationFee + InvoiceRegionalFees + InvoiceOptions + InvoiceOptionsRegionalFees)
                                - (InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + InvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive),

        TotalOffsetMinusDealershipIncentives =
                                             TotalOffset
                                             - DealershipIncentives
                                             - DealershipToCustomerIncentives,

        UpfrontTotalOffsetMinusDealershipIncentives =
                                                    UpfrontTotalOffset
                                                    - DealershipIncentives
                                                    - DealershipToCustomerIncentives,

        ConfiguratorBase =
                         InvoiceBase + ConfiguratorTotalOffset - DealershipIncentives - DealershipToCustomerIncentives,

      ConfiguratorOpenNetworkPremium,
        ConfiguratorMemberNetworkOffset =
                                        ConfiguratorTotalOffset - ConfiguratorOpenNetworkPremium,
      ConfiguratorTotalOffset,

        ConfiguratorTotal =
                          InvoiceBase + DestinationFee + LowestInvoiceRegionalFees + ConfiguratorTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + LowestInvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - LowestMemberNetworkIncentive,

        ConfiguratorNoOptionsTotal =
                                   InvoiceBase + DestinationFee + LowestInvoiceRegionalFees + ConfiguratorTotalOffset - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - LowestMemberNetworkIncentive,

        ConfiguratorTotalSavingsFromSticker =
                                            (StickerBase + DestinationFee + LowestStickerRegionalFees + StickerOptions)
                                            - (InvoiceBase + DestinationFee + LowestInvoiceRegionalFees + ConfiguratorTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + LowestInvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - LowestMemberNetworkIncentive),

        ConfiguratorTotalSavingsFromStickerPercent =
                                                   (
                                                     ((StickerBase + DestinationFee + LowestStickerRegionalFees + StickerOptions)
                                                      - (InvoiceBase + DestinationFee + LowestInvoiceRegionalFees + ConfiguratorTotalOffset + InvoiceOptions + isnull(@InstalledOptions, 0.00) + LowestInvoiceOptionsRegionalFees - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - LowestMemberNetworkIncentive))
                                                     / nullif((StickerBase + DestinationFee + LowestStickerRegionalFees + StickerOptions), 0)
                                                   ) * 100,

        MinimumEstimatedSavingsNoOffset =
                                        (StickerBase + DestinationFee + StickerRegionalFees)
                                        - (InvoiceBase + DestinationFee + InvoiceRegionalFees),

        MinimumEstimatedSavings =
                                (StickerBase + DestinationFee + StickerRegionalFees)
                                - (InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive),

        MinimumEstimatedSavingsPercentFromSticker =
                                                  (
                                                    ((StickerBase + DestinationFee + StickerRegionalFees)
                                                     - (InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset - CustomerIncentives - DealershipIncentives - ProgramIncentives - DealershipToCustomerIncentives - FinanceAndLeaseIncentives - MemberNetworkIncentive))
                                                    / nullif((StickerBase + DestinationFee + StickerRegionalFees), 0)
                                                  ) * 100,

        GuaranteedSavings =
                          (StickerBase + DestinationFee + StickerRegionalFees)
                          - (InvoiceBase + DestinationFee + InvoiceRegionalFees + TotalOffset - PreSelectedCustomerIncentives - DealershipIncentives - DealershipToCustomerIncentives),

        ConfiguratorPriceType =
                              CASE
                              WHEN (PriceExperience = 'Upfront' AND @ProgramAffinityGroupId = 120) OR PriceExperience = 'Public' THEN 'ConfiguredHistorical'
                              WHEN PriceExperience = 'Upfront' THEN 'Current'
                              ELSE NULL
                              END,

        PriceType =
                  CASE
                  When PriceEmphasis = 'priceTotalWithDealerFeesAndAccessories' and ShowTransparencyPledge = 0 then 'Excluded'
                  When PriceEmphasis = 'priceTotalPlusDocumentationFee' and (DocumentationFee is null or IsExcluded = 1) then 'Excluded'
                  WHEN PriceExperience = 'Restricted' THEN 'Restricted'
                  WHEN IsExcluded = 1 THEN 'Excluded'
                  ELSE 'Upfront'
                  END,

        ProgramIncentivesSavingsFromStickerPercent =
                                                   (ISNULL(ProgramIncentives, 0) / NULLIF(StickerBase + DestinationFee + StickerRegionalFees + StickerOptions, 0)) * 100.,

      CustomerIncentives,
        DealershipIncentives =
                             DealershipIncentives + DealershipToCustomerIncentives,
      ProgramIncentives,
      CustomerAndProgramIncentives,
      PreSelectedCustomerIncentives,
      PostSelectedCustomerIncentives,
      DealershipToCustomerIncentives,
      MemberNetworkIncentive,
      MemberNetworkIncentivePagId,
      FinanceAndLeaseIncentives,
        AdditionalIncentives =
                             PostSelectedCustomerIncentives + ProgramIncentives + MemberNetworkIncentive,
        TotalIncentives =
                        CustomerIncentives + DealershipIncentives + ProgramIncentives + DealershipToCustomerIncentives + FinanceAndLeaseIncentives + MemberNetworkIncentive,

      MessageBlockId,
      IsOpenNetworkFlag,
      MemberProgramMax,
      DestinationFee,
      DocumentationFee,
      DocumentationFeeStateMax,
      MakeId,
      ModelId,
      DealershipState,
      DealershipPostalCode,
      ExteriorColorId,

        CASE
        When PriceEmphasis = 'priceTotalWithDealerFeesAndAccessories' and (DocumentationFee is NULL OR HasPreInstall IS NULL OR HasOtherFees IS NULL OR IsExcluded = 1) then 1
        When PriceEmphasis = 'priceTotalPlusDocumentationFee' and (DocumentationFee is null OR IsExcluded = 1) then 1
        else IsExcluded
        end IsExcludedFlag,

      IsPreselectedFlag,

      DsaVersionId,
      ExpectedRevenueBaseA,
      ExpectedRevenueBaseB,
      ExpectedRevenue,

      DriveDistance,
      DriveTime,

      ExpectedRevenueDistanceRank,

      IsTexasZoneFlag,
      IsDefaultDealershipFlag,

      Distance,
      IncludeTexasFlag,

      AbTests,
      IsAbpActiveFlag,
      HasPriceReportFlag,
      NewerModelYearTrimId,
      AlternateABPActiveTrimId,
      AlternateABPActiveTrimYear,
      IsMarketResearch,

      TotalOffsetRank,
      RegionIds,
      ShowDocumentationFeeFlag,
      ConsumerReportsRecommendId,
      DocumentationFeeName,
      EditorialTargetTotalOffset,
      IsConsideredForPriceExperience,
      HistoricalAverageMemberOffset,
      HistoricalTotalOffset,
      IsClosestFlag,
      IsDisplayedFlag,
      IsLowestFlag,
      SortOrder,
      AvailableDealershipCount,
      DsaTotal,
      SearchState,
      PriceEmphasis,
      TrueCarDiscountPercent,
      HasPreInstall,
      HasTrueCarDiscount,
      DealershipAccessoryTotal,
      DealershipDiscountedAccessoryTotal,
      HasOtherFees,
      GenericStyle,
        DealershipFeesTotal =
                            DealershipFeeTotal + DocumentationFee,
        DealershipOtherFeesTotal =
                                 DealershipFeeTotal,
        DealershipFeesTotalPlusDealershipAccessoryTotal  =
                                                         DealershipAccessoryTotal + DealershipFeeTotal + DocumentationFee,
        DealershipFeesTotalPlusDealershipDiscountedAccessoryTotal =
                                                                  DealershipDiscountedAccessoryTotal + DealershipFeeTotal + DocumentationFee,
      ShowTransparencyPledge
    FROM
      (
        SELECT
          TrimId,
          DealershipId,

          StickerBase,
            StickerRegionalFees =
                                CASE
                                WHEN PriceExperience IN ('Restricted', 'Excluded', 'Public') OR IsExcluded = 1 THEN LowestStickerRegionalFees
                                ELSE StickerRegionalFees
                                END,
          LowestStickerRegionalFees,
            UpfrontStickerRegionalFees =
                                       StickerRegionalFees,
          StickerOptions,

          IsInvoiceVisibleFlag,
          InvoiceBase,
          InvoiceOptions,
            InvoiceRegionalFees =
                                CASE
                                WHEN PriceExperience IN ('Restricted', 'Excluded', 'Public') OR IsExcluded = 1 THEN LowestInvoiceRegionalFees
                                ELSE InvoiceRegionalFees
                                END,
          LowestInvoiceRegionalFees,
            UpfrontInvoiceRegionalFees =
                                       InvoiceRegionalFees,
            InvoiceOptionsRegionalFees =
                                       CASE
                                       WHEN PriceExperience IN ('Restricted', 'Excluded', 'Public') OR IsExcluded = 1 THEN LowestInvoiceOptionsRegionalFees
                                       ELSE InvoiceOptionsRegionalFees
                                       END,
          LowestInvoiceOptionsRegionalFees,
            UpfrontInvoiceOptionsRegionalFees =
                                              InvoiceOptionsRegionalFees,
          MemberNetworkOffset,
          OpenNetworkPremium,

          CustomerIncentives,
          DealershipIncentives,
          ProgramIncentives,
          CustomerAndProgramIncentives,
          PreSelectedCustomerIncentives,
          PostSelectedCustomerIncentives,

          DealershipToCustomerIncentives,
          MemberNetworkIncentive,
          LowestMemberNetworkIncentive,
          MemberNetworkIncentivePagId,
          FinanceAndLeaseIncentives,

          MessageBlockId,
          IsOpenNetworkFlag,
          MemberProgramMax,
          DestinationFee,
          DocumentationFee,
          DocumentationFeeStateMax,
          MakeId,
          ModelId,
          DealershipState,
          DealershipPostalCode,
          ExteriorColorId,

          IsExcluded,

          IsPreselectedFlag,

          DsaVersionId,
          ExpectedRevenueBaseA,
          ExpectedRevenueBaseB,
          ExpectedRevenue,

          DriveDistance,
          DriveTime,

          ExpectedRevenueDistanceRank,

          IsTexasZoneFlag,
          IsDefaultDealershipFlag,
          IsOemExperienceFlag,

          Distance,
          IncludeTexasFlag,

          AbTests,
          IsAbpActiveFlag,
          HasPriceReportFlag,
          NewerModelYearTrimId,
          AlternateABPActiveTrimId,
          AlternateABPActiveTrimYear,
          IsMarketResearch,

          TotalOffsetRank,
          RegionIds,
          ShowDocumentationFeeFlag,
          ConsumerReportsRecommendId,
          DocumentationFeeName,

          PriceExperience,

          ConfiguratorTotalOffset,

            TotalOffset =
                        CASE
                        WHEN PriceExperience in ('Restricted', 'Excluded') OR IsExcluded = 1 THEN ISNULL(ConfiguratorTotalOffset, EditorialTargetTotalOffset)
                        ELSE TotalOffset
                        END,
            UpfrontTotalOffset = TotalOffset,

          EditorialTargetTotalOffset,
          IsConsideredForPriceExperience,
          ConfiguratorOpenNetworkPremium,
          HistoricalAverageMemberOffset,
          HistoricalTotalOffset,
          IsClosestFlag,
          IsDisplayedFlag,
          IsLowestFlag,
          SortOrder,
          AvailableDealershipCount,
          DsaTotal,
          SearchState,
          PriceEmphasis,
          TrueCarDiscountPercent,
          HasPreInstall,
          HasTrueCarDiscount,
          DealershipAccessoryTotal,
          DealershipDiscountedAccessoryTotal,
          HasOtherFees,
          DealershipFeeTotal,
          GenericStyle,
          DealershipPriceEmphasis,
          ShowTransparencyPledge
        FROM
          (
            SELECT
              X.TrimId,
              X.DealershipId,
              StickerBase,

              IsInvoiceVisibleFlag,
              InvoiceBase,

                CustomerIncentives = ISNULL(I.CustomerIncentives, 0),
                DealershipIncentives = ISNULL(I.DealershipIncentives, 0) + ISNULL(@InstalledIncentives, 0.00),
                ProgramIncentives = ISNULL(I.ProgramIncentives, 0),
                CustomerAndProgramIncentives = ISNULL(I.CustomerIncentives, 0) + ISNULL(I.ProgramIncentives, 0),
                DealershipToCustomerIncentives = ISNULL(DI.DealershipToCustomerIncentives, 0),
                FinanceAndLeaseIncentives = ISNULL(I.FinanceAndLeaseIncentives, 0),
                PreSelectedCustomerIncentives = ISNULL(I.PreSelectedCustomerIncentives, 0),
                PostSelectedCustomerIncentives = ISNULL(I.PostSelectedCustomerIncentives, 0),

                ISNULL(MemberNetworkIncentive, 0) MemberNetworkIncentive,
                LowestMemberNetworkIncentive =
                                             MIN(
                                                 CASE WHEN TotalOffsetRank = 1 THEN
                                                   ISNULL(MemberNetworkIncentive, 0)
                                                 ELSE null
                                                 END
                                             ) OVER (partition BY X.TrimId),

              MemberNetworkIncentivePagId,

              TotalOffsetRank,

              TotalOffset,
              IsOpenNetworkFlag,
              MemberProgramMax,
              MemberNetworkOffset,
              OpenNetworkPremium,
              PriceExperience,

              MessageBlockId,
              DestinationFee,
              DocumentationFee,
              DocumentationFeeStateMax,

              X.MakeId,
              ModelId,
              DealershipState,
              ExteriorColorId,
              IsExcluded,
              DealershipPostalCode,

              IsPreselectedFlag,
              DsaVersionId,
              ExpectedRevenueBaseA,
              ExpectedRevenueBaseB,
              ExpectedRevenue,
              DriveDistance,
              DriveTime,
              ExpectedRevenueDistanceRank,
              DealershipMedianNumber,

              IsTexasZoneFlag,
              IsDefaultDealershipFlag,
              Distance,
              IncludeTexasFlag,
              IsOemExperienceFlag,

              ConfiguratorPriceType,
              AbTests,
              IsAbpActiveFlag,
              HasPriceReportFlag,
              NewerModelYearTrimId,
              AlternateABPActiveTrimId,
              AlternateABPActiveTrimYear,
              IsMarketResearch,

              RegionIds,
              ShowDocumentationFeeFlag,
              ConsumerReportsRecommendId,
              DocumentationFeeName,

              EditorialTargetTotalOffset,
              HistoricalAverageMemberOffset,

                StickerRegionalFees =
                                    CASE
                                    WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(StickerRegionalFees, 0)
                                    WHEN @IsUnConfiguredFlag = 0 AND X.DealershipId IS NOT NULL THEN ISNULL(StickerRegionalFeesTotal, 0)
                                    ELSE ISNULL(StickerRegionalFeeFlat, 0)
                                    END,

                LowestStickerRegionalFees =
                                          MIN(
                                              CASE WHEN TotalOffsetRank = 1 THEN
                                                CASE
                                                WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(StickerRegionalFees, 0)
                                                WHEN @IsUnConfiguredFlag = 0 AND X.DealershipId IS NOT NULL THEN ISNULL(StickerRegionalFeesTotal, 0)
                                                ELSE ISNULL(RAFT.StickerRegionalFeeFlat, 0)
                                                END
                                              ELSE null
                                              END
                                          ) OVER (partition BY X.TrimId),

                StickerOptions =
                               CASE
                               WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(InteriorColorSticker, 0) + ISNULL(ExteriorColorSticker, 0) + ISNULL(OptionsSticker, 0)
                               ELSE ISNULL(StickerColorTotal, 0) + ISNULL(StickerOptionTotal, 0)
                               END,

                InvoiceRegionalFees =
                                    CASE
                                    WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceRegionalFees, 0)
                                    WHEN @IsUnConfiguredFlag = 0 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceRegionalFeesTotal, 0)
                                    ELSE ISNULL(InvoiceRegionalFeeBase, 0) + ISNULL(InvoiceRegionalFeeFlat, 0)
                                    END,

                InvoiceOptions =
                               CASE
                               WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(InteriorColorInvoice, 0) + ISNULL(ExteriorColorInvoice, 0) + ISNULL(OptionsInvoice, 0)
                               ELSE ISNULL(InvoiceColorTotal, 0) + ISNULL(InvoiceOptionTotal, 0)
                               END,

                InvoiceOptionsRegionalFees =
                                           CASE
                                           WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceOptionsRegionalFees, 0)
                                           WHEN @IsUnConfiguredFlag = 0 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceOptionsRegionalFeesTotal, 0)
                                           ELSE ISNULL(InvoiceOptionsRegionalFee, 0)
                                           END,

                LowestInvoiceRegionalFees =
                                          MIN(
                                              CASE WHEN TotalOffsetRank = 1 THEN
                                                CASE
                                                WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceRegionalFees, 0)
                                                WHEN @IsUnConfiguredFlag = 0 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceRegionalFeesTotal, 0)
                                                ELSE ISNULL(RAFT.InvoiceRegionalFeeBase, 0) + ISNULL(RAFT.InvoiceRegionalFeeFlat, 0)
                                                END
                                              ELSE null
                                              END
                                          ) OVER (partition BY X.TrimId),

                LowestInvoiceOptionsRegionalFees =
                                                 MIN(
                                                     CASE WHEN TotalOffsetRank = 1 THEN
                                                       CASE
                                                       WHEN @IsUnConfiguredFlag = 1 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceOptionsRegionalFees, 0)
                                                       WHEN @IsUnConfiguredFlag = 0 AND X.DealershipId IS NOT NULL THEN ISNULL(InvoiceOptionsRegionalFeesTotal, 0)
                                                       ELSE ISNULL(RAFT.InvoiceOptionsRegionalFee, 0)
                                                       END
                                                     ELSE null
                                                     END
                                                 ) OVER (partition BY X.TrimId),

              IsConsideredForPriceExperience,
              ConfiguratorOpenNetworkPremium,
              HistoricalTotalOffset,
              ConfiguratorTotalOffset,
              IsClosestFlag,
              IsDisplayedFlag,
              IsLowestFlag,
              SortOrder,
              AvailableDealershipCount,
              DsaTotal,
              SearchState,
              PriceEmphasis,
              TrueCarDiscountPercent,
              HasPreInstall,
              HasTrueCarDiscount,
              DealershipAccessoryTotal,
              DealershipDiscountedAccessoryTotal,
              HasOtherFees,
              DealershipFeeTotal,
              GenericStyle,
              DealershipPriceEmphasis,
              ShowTransparencyPledge
            FROM
              (
                SELECT
                  MakeId,
                  ModelId,
                  W.TrimId,
                  InvoiceBase,
                  StickerBase,
                  ExteriorColorId,
                  InteriorColorId,
                  ExteriorColorSticker,
                  InteriorColorSticker,
                  ExteriorColorInvoice,
                  InteriorColorInvoice,
                  OptionsSticker,
                  OptionsInvoice,
                  MemberProgramMax,
                  DestinationFee,
                  IsOemExperienceFlag,
                  IsAbpActiveFlag,
                  HasPriceReportFlag,
                  NewerModelYearTrimId,
                  AlternateABPActiveTrimId,
                  AlternateABPActiveTrimYear,
                  ConsumerReportsRecommendId,
                  UseInternalIncentives,

                  IsOpenNetworkFlag,
                  HasFinancing,
                  AllowSavingsFinder,

                  OpenNetworkOffsetDefault,
                  OpenNetworkOffsetMin,

                  DealershipState,
                  DealershipPostalCode,
                  DocumentationFee,
                  RegionIds,
                  DealershipId,
                  DsaVersionId,
                  Distance,
                  ExpectedRevenueBaseA,
                  ExpectedRevenueBaseB,
                  ExpectedRevenue,
                  AbTests,
                  DriveDistance,
                  DriveTime,
                  IncludeTexasFlag,
                  IsTexasZoneFlag,
                  DealershipIsPreSelectedDistance,
                  IsDefaultDealershipFlag,

                  StickerRegionalFees,
                  InvoiceRegionalFees,
                  InvoiceOptionsRegionalFees,
                  MessageBlockId,
                  MemberNetworkOffset,
                  IsExcluded,

                  OpenNetworkPremium,
                  ConfiguratorOpenNetworkPremium,
                  TotalOffset,
                  EditorialTargetTotalOffset,

                  DocumentationFeeStateMax,
                  IsMarketResearch,
                  ConfiguratorPriceType,
                  ShowDocumentationFeeFlag,
                  DocumentationFeeName,
                  ExpectedRevenueDistanceRank,
                  IsPreselectedFlag,

                  ScoreMin,
                  ScoreMax,

                  TotalOffsetRank,

                  IsInvoiceVisibleFlag,
                  PriceExperience,
                  IsConsideredForPriceExperience,

                  CustomerIncentives,
                  DealershipIncentives,
                  ProgramIncentives,
                  DealershipToCustomerIncentives,
                  FinanceAndLeaseIncentives,

                    ConfiguratorTotalOffset =
                                            ISNULL(
                                                MAX(
                                                    CASE
                                                    WHEN (PriceExperience in ('Public') OR (PriceExperience in ('Upfront') AND @IsLoggedIn = 0 AND @ProgramAffinityGroupId = 120)) AND TotalOffsetRank = 1 THEN HistoricalTotalOffset
                                                    WHEN PriceExperience in ('Upfront') AND TotalOffsetRank = 1 THEN TotalOffset
                                                    WHEN PriceExperience in ('Restricted', 'Excluded') AND TotalOffsetRank = 1 and MemberNetworkOffset > ISNULL(STO.LocalTargetOffset, TargetOffset) and IsExcluded = 0 THEN MemberNetworkOffset + ConfiguratorOpenNetworkPremium
                                                    WHEN PriceExperience in ('Restricted', 'Excluded') AND TotalOffsetRank = 1 THEN ISNULL(STO.LocalTargetOffset, TargetOffset) + ConfiguratorOpenNetworkPremium
                                                    END
                                                ) OVER (partition BY W.TrimId),
                                                EditorialTargetTotalOffset),

                  HAMO.HistoricalAverageMemberOffset,
                  HTO.HistoricalTotalOffset,
                  W.PricingTrimGroupId,

                    DealershipMedianNumber =
                                           ISNULL(REPLACE(CONVERT(SMALLINT, ROUND(DealershipCount / 2., 0, 1)), 0, 1), 1),
                  IsClosestFlag,
                    IsDisplayedFlag =
                                    CASE WHEN DsaVersionId = 2 OR (@BestPriceQuote = 1 AND SortOrder <= 1) or (@BestPriceQuote = 0 and (SortOrder <= 3 OR IsTexasZoneFlag = 1 OR DealershipId IS null)) THEN 1 ELSE 0 END,
                  IsLowestFlag,
                  SortOrder,
                  AvailableDealershipCount,
                  DsaTotal,
                  SearchState,
                    PriceEmphasis = CASE when PriceEmphasisMax = 2 then 'priceTotalWithDealerFeesAndAccessories' when PriceEmphasisMax = 1 then 'priceTotalPlusDocumentationFee' else 'priceTotal' end,
                  TrueCarDiscountPercent,
                  HasPreInstall,
                  HasTrueCarDiscount,
                  DealershipAccessoryTotal,
                  DealershipDiscountedAccessoryTotal,
                  HasOtherFees,
                  DealershipFeeTotal,
                  GenericStyle,
                  DealershipPriceEmphasis,
                  ShowTransparencyPledge
                from
                  (
                    SELECT
                      MakeId,
                      ModelId,
                      TrimId,
                      InvoiceBase,
                      StickerBase,
                      ExteriorColorId,
                      InteriorColorId,
                      ExteriorColorSticker,
                      InteriorColorSticker,
                      ExteriorColorInvoice,
                      InteriorColorInvoice,
                      OptionsSticker,
                      OptionsInvoice,
                      MemberProgramMax,
                      DestinationFee,
                      IsOemExperienceFlag,
                      IsAbpActiveFlag,
                      HasPriceReportFlag,
                      NewerModelYearTrimId,
                      AlternateABPActiveTrimId,
                      AlternateABPActiveTrimYear,
                      ConsumerReportsRecommendId,
                      UseInternalIncentives,

                      IsOpenNetworkFlag,
                      HasFinancing,
                      AllowSavingsFinder,

                      OpenNetworkOffsetDefault,
                      OpenNetworkOffsetMin,

                      DealershipState,
                      DealershipPostalCode,
                      DocumentationFee,
                      RegionIds,
                      DealershipId,
                      DsaVersionId,
                      Distance,
                      ExpectedRevenueBaseA,
                      ExpectedRevenueBaseB,
                      ExpectedRevenue,
                      AbTests,
                      DriveDistance,
                      DriveTime,
                      IncludeTexasFlag,
                      IsTexasZoneFlag,
                      DealershipIsPreSelectedDistance,
                      IsDefaultDealershipFlag,

                      StickerRegionalFees,
                      InvoiceRegionalFees,
                      InvoiceOptionsRegionalFees,
                      MessageBlockId,
                      MemberNetworkOffset,
                      IsExcluded,

                      OpenNetworkPremium,
                      ConfiguratorOpenNetworkPremium,
                      TotalOffset,
                      EditorialTargetTotalOffset,

                      DocumentationFeeStateMax,
                      IsMarketResearch,
                      ConfiguratorPriceType,
                      ShowDocumentationFeeFlag,
                      DocumentationFeeName,
                      ExpectedRevenueDistanceRank,
                      IsPreselectedFlag,

                      TotalOffsetRank,

                      CustomerIncentives,
                      DealershipIncentives,
                      ProgramIncentives,
                      DealershipToCustomerIncentives,
                      FinanceAndLeaseIncentives,
                      PricingTrimGroupId,

                      SortOrder,
                      IsClosestFlag,
                      IsLowestFlag,

                      CASE WHEN SortOrder <= 3 OR IsTexasZoneFlag = 1 OR DealershipId IS null THEN 1 ELSE 0 END IsConsideredForPriceExperience,
                      sum(CASE WHEN SortOrder <= 3 OR IsTexasZoneFlag = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY TrimId) DealershipCount,
                      COUNT(DealershipId) OVER (PARTITION BY TrimId) AvailableDealershipCount,

                      MIN(CASE WHEN SortOrder <= 3 OR IsTexasZoneFlag = 1 OR DealershipId IS null THEN SC.PriceExperienceAndIsInvoiceVisibleScore end) OVER (PARTITION BY TrimId) ScoreMin,
                      MAX(CASE WHEN SortOrder <= 3 OR IsTexasZoneFlag = 1 OR DealershipId IS null THEN SC.PriceExperienceAndIsInvoiceVisibleScore end) OVER (PARTITION BY TrimId) ScoreMax,

                      MAX(CASE
                          WHEN (SortOrder <= 3 OR IsTexasZoneFlag = 1 OR DealershipId IS null) and (DealershipPriceEmphasis = 'priceTotalWithDealerFeesAndAccessories' or SearchPostalCodePriceEmphasis = 'priceTotalWithDealerFeesAndAccessories') then 2
                          WHEN (SortOrder <= 3 OR IsTexasZoneFlag = 1 OR DealershipId IS null) and (DealershipPriceEmphasis = 'priceTotalPlusDocumentationFee' or SearchPostalCodePriceEmphasis = 'priceTotalPlusDocumentationFee') then 1
                          else 0
                          END) OVER (PARTITION BY TrimId) PriceEmphasisMax,
                      DsaTotal,
                      SearchState,
                      TargetOffset,
                      TrueCarDiscountPercent,
                      HasPreInstall,
                      HasTrueCarDiscount,
                      DealershipAccessoryTotal,
                      DealershipDiscountedAccessoryTotal,
                      HasOtherFees,
                      DealershipFeeTotal,
                      GenericStyle,
                      DealershipPriceEmphasis,
                      ShowTransparencyPledge
                    from
                      (
                        SELECT
                          MakeId,
                          ModelId,
                          TrimId,
                          InvoiceBase,
                          StickerBase,
                          ExteriorColorId,
                          InteriorColorId,
                          ExteriorColorSticker,
                          InteriorColorSticker,
                          ExteriorColorInvoice,
                          InteriorColorInvoice,
                          OptionsSticker,
                          OptionsInvoice,
                          MemberProgramMax,
                          DestinationFee,
                          IsOemExperienceFlag,
                          IsAbpActiveFlag,
                          HasPriceReportFlag,
                          NewerModelYearTrimId,
                          AlternateABPActiveTrimId,
                          AlternateABPActiveTrimYear,
                          ConsumerReportsRecommendId,
                          UseInternalIncentives,

                          IsOpenNetworkFlag,
                          HasFinancing,
                          AllowSavingsFinder,

                          OpenNetworkOffsetDefault,
                          OpenNetworkOffsetMin,

                          DealershipState,
                          DealershipPostalCode,
                          DocumentationFee,
                          RegionIds,
                          DealershipId,
                          DsaVersionId,
                          Distance,
                          ExpectedRevenueBaseA,
                          ExpectedRevenueBaseB,
                          ExpectedRevenue,
                          AbTests,
                          DriveDistance,
                          DriveTime,
                          IncludeTexasFlag,
                          IsTexasZoneFlag,
                          DealershipIsPreSelectedDistance,
                          IsDefaultDealershipFlag,

                          StickerRegionalFees,
                          InvoiceRegionalFees,
                          InvoiceOptionsRegionalFees,
                          MessageBlockId,
                          MemberNetworkOffset,
                          IsExcluded,

                          OpenNetworkPremium,
                          ConfiguratorOpenNetworkPremium,
                          TotalOffset,
                          EditorialTargetTotalOffset,

                          DocumentationFeeStateMax,
                          IsMarketResearch,
                          ConfiguratorPriceType,
                          ShowDocumentationFeeFlag,
                          DocumentationFeeName,
                          ExpectedRevenueDistanceRank,
                          IsPreselectedFlag,

                          TotalOffsetRank,

                          CustomerIncentives,
                          DealershipIncentives,
                          ProgramIncentives,
                          DealershipToCustomerIncentives,
                          FinanceAndLeaseIncentives,

                          DealershipIsInvoiceVisibleFlag,
                          SearchPostalCodeIsInvoiceVisibleFlag,
                          DealershipPriceExperience,
                          SearchPostalCodePriceExperience,
                          DealershipPriceEmphasis,
                          SearchPostalCodePriceEmphasis,
                          PricingTrimGroupId,

                          ROW_NUMBER() over (PARTITION BY TrimId ORDER by
                            CASE WHEN DealershipId is null or DriveTime <= DriveTimeMax or IsTexasZoneFlag = 1 THEN 0 ELSE 1 end,
                            case WHEN (@BestPriceQuote = 1 AND TotalOffsetRank = 1) or (@BestPriceQuote = 0 AND DrivetimeRank = 1) then 0 ELSE 1 END,
                            case WHEN (@BestPriceQuote = 1 AND DrivetimeRank = 1) or (@BestPriceQuote = 0 AND TotalOffsetRank = 1) then 0 ELSE 1 END,
                            CASE when ExpectedRevenueDistanceRank = 1 then 0 ELSE 1 END,
                            CASE when ExpectedRevenueDistanceRank = 2 then 0 ELSE 1 END,
                            CASE when ExpectedRevenueDistanceRank = 3 then 0 ELSE 1 END,
                            DrivetimeRank
                          ) SortOrder,

                          CASE WHEN TotalOffsetRank = 1 THEN 1 ELSE 0 END IsLowestFlag,

                          IsClosestFlag,
                          DsaTotal,
                          SearchState,
                          TargetOffset,
                          TrueCarDiscountPercent,
                          HasPreInstall,
                          HasTrueCarDiscount,
                          DealershipAccessoryTotal = CASE WHEN HasValidAccessories = 1 THEN ISNULL(DealershipAccessoryTotal, 0) ELSE DealershipAccessoryTotal end,
                          DealershipDiscountedAccessoryTotal = CASE WHEN HasValidAccessories = 1 THEN ISNULL(DealershipDiscountedAccessoryTotal, 0) ELSE DealershipDiscountedAccessoryTotal end,
                          HasOtherFees,
                          DealershipFeeTotal = CASE WHEN HasValidFees = 1 THEN ISNULL(DealershipFeeTotal, 0) ELSE DealershipFeeTotal end,
                          GenericStyle,
                          ShowTransparencyPledge
                        FROM
                          (
                            SELECT
                              T.MakeId,
                              T.ModelId,
                              T.TrimId,
                              T.InvoiceBase,
                              T.StickerBase,
                              T.ExteriorColorId,
                              T.InteriorColorId,
                              T.ExteriorColorSticker,
                              T.InteriorColorSticker,
                              T.ExteriorColorInvoice,
                              T.InteriorColorInvoice,
                              T.OptionsSticker,
                              T.OptionsInvoice,
                              T.MemberProgramMax,
                              T.DestinationFee,
                              T.PricingTrimGroupId,
                              T.IsOemExperienceFlag,
                              T.IsAbpActiveFlag,
                              T.HasPriceReportFlag,
                              T.NewerModelYearTrimId,
                              T.AlternateABPActiveTrimId,
                              T.AlternateABPActiveTrimYear,
                              T.ConsumerReportsRecommendId,
                              T.UseInternalIncentives,

                              PAG.OpenMarketFlag IsOpenNetworkFlag,
                              PAG.HasFinancing,
                              PAG.AllowSavingsFinder,
                              PC.[State] SearchState,

                              OL.open_market_offset_default OpenNetworkOffsetDefault,
                              OL.open_market_offset_min OpenNetworkOffsetMin,

                              D.DealershipState,
                              D.DealershipPostalCode,
                              D.DocumentationFee,
                              D.RegionIds,
                              D.DealershipId,
                              D.DsaVersionId,
                              D.Distance,
                              D.ExpectedRevenueBaseA,
                              D.ExpectedRevenueBaseB,
                              D.ExpectedRevenue,
                              D.AbTests,
                              D.DriveDistance,
                              D.DriveTime,
                              D.IncludeTexasFlag,
                              D.IsTexasZoneFlag,
                              D.DealershipIsPreSelectedDistance,
                              D.IsDefaultDealershipFlag,

                              P.StickerRegionalFees,
                              P.InvoiceRegionalFees,
                              P.InvoiceOptionsRegionalFees,
                              P.MessageBlockId,
                              MNO.MemberNetworkOffset,
                              MNO.IsExcluded,

                              OpenNetworkPremium =
                                                 CASE WHEN PAG.OpenMarketFlag = 1 THEN ISNULL(D.OpenNetworkPremium, OL.open_market_offset_default) ELSE 0 END,

                              ConfiguratorOpenNetworkPremium =
                                                             CASE WHEN PAG.OpenMarketFlag = 1 THEN OL.open_market_offset_min ELSE 0 END,

                              TotalOffset =
                                          MNO.MemberNetworkOffset
                                          + CASE WHEN PAG.OpenMarketFlag = 1 THEN ISNULL(D.OpenNetworkPremium, OL.open_market_offset_default) ELSE 0 END,

                              EditorialTargetTotalOffset =
                                                         T.TargetOffset
                                                         + CASE WHEN PAG.OpenMarketFlag = 1 THEN OL.open_market_offset_min ELSE 0 END,

                              DocumentationFeeStateMax =
                                                       CASE WHEN D.DealershipId IS NOT NULL THEN DSR.DocumentationFeeStateMax ELSE SR.DocumentationFeeStateMax END,

                              IsMarketResearch =
                                               ISNULL(DSR.IsMarketResearch, SR.IsMarketResearch),

                              ConfiguratorPriceType =
                                                    ISNULL(DSR.ConfiguratorPriceType, SR.ConfiguratorPriceType),

                              ShowDocumentationFeeFlag =
                                                       COALESCE(DSR.ShowDocumentationFeeFlag, SR.ShowDocumentationFeeFlag, 0),

                              DocumentationFeeName =
                                                   COALESCE(D.DocumentationFeeName, DSR.DocumentationFeeName, SR.DocumentationFeeName),

                              ExpectedRevenueDistanceRank =
                                                          ISNULL(D.ExpectedRevenueDistanceRank, 1),

                              IsPreselectedFlag =
                                                CASE WHEN D.DriveDistance <= D.DealershipIsPreSelectedDistance OR D.IsTexasZoneFlag = 1 THEN 1 ELSE 0 END,

                              TotalOffsetRank =
                                              ROW_NUMBER() OVER (partition BY T.TrimId ORDER BY
                                                CASE WHEN D.DealershipId IS null THEN 0 ELSE 1 END,
                                                CASE WHEN D.DriveTime <= D.DriveTimeMax THEN 0 ELSE 1 end,
                                                case WHEN D.DocumentationFee is not NULL and DF.HasOtherFees is not NULL and DA.HasPreInstall is not null then 0 ELSE 1 END,
                                                MNO.IsExcluded,
                                                InvoiceBase
                                                + DestinationFee
                                                + ISNULL(P.InvoiceRegionalFees, 0)
                                                + MNO.MemberNetworkOffset
                                                + CASE WHEN PAG.OpenMarketFlag = 1 THEN ISNULL(D.OpenNetworkPremium, OL.open_market_offset_default) ELSE 0 END
                                                + ISNULL(T.InteriorColorInvoice, 0) + ISNULL(T.ExteriorColorInvoice, 0) + ISNULL(T.OptionsInvoice, 0)
                                                + isnull(@InstalledOptions, 0)
                                                + ISNULL(P.InvoiceOptionsRegionalFees, 0)
                                                - ISNULL(P.CustomerIncentives, 0)
                                                - ISNULL(P.DealershipIncentives, 0)
                                                - ISNULL(TPI.ProgramIncentives, 0)
                                                - ISNULL(P.DealershipToCustomerIncentives, 0)
                                                - ISNULL(P.FinanceAndLeaseIncentives, 0)
                                                + ISNULL(D.DocumentationFee, 0)
                                                + ISNULL(DA.DealershipDiscountedAccessoryTotal, 0)
                                                + ISNULL(DF.DealershipFeeTotal, 0),
                                                DrivetimeRank),

                              DsaTotal =
                                       InvoiceBase
                                       + DestinationFee
                                       + ISNULL(P.InvoiceRegionalFees, 0)
                                       + MNO.MemberNetworkOffset
                                       + CASE WHEN PAG.OpenMarketFlag = 1 THEN ISNULL(D.OpenNetworkPremium, OL.open_market_offset_default) ELSE 0 END
                                       + ISNULL(T.InteriorColorInvoice, 0) + ISNULL(T.ExteriorColorInvoice, 0) + ISNULL(T.OptionsInvoice, 0)
                                       + isnull(@InstalledOptions, 0)
                                       + ISNULL(P.InvoiceOptionsRegionalFees, 0)
                                       - ISNULL(P.CustomerIncentives, 0)
                                       - ISNULL(P.DealershipIncentives, 0)
                                       - ISNULL(TPI.ProgramIncentives, 0)
                                       - ISNULL(P.DealershipToCustomerIncentives, 0)
                                       - ISNULL(P.FinanceAndLeaseIncentives, 0)
                                       + ISNULL(D.DocumentationFee, 0)
                                       + ISNULL(DA.DealershipDiscountedAccessoryTotal, 0)
                                       + ISNULL(DF.DealershipFeeTotal, 0),

                              P.CustomerIncentives,
                              P.DealershipIncentives,
                              TPI.ProgramIncentives,
                              P.DealershipToCustomerIncentives,
                              P.FinanceAndLeaseIncentives,

                              DSR.IsInvoiceVisibleFlag DealershipIsInvoiceVisibleFlag,
                              SR.IsInvoiceVisibleFlag SearchPostalCodeIsInvoiceVisibleFlag,
                              DSR.PriceExperience DealershipPriceExperience,
                              SR.PriceExperience SearchPostalCodePriceExperience,
                              ISNULL(DSR.PriceEmphasis, 'priceTotal') DealershipPriceEmphasis,
                              ISNULL(SR.PriceEmphasis, 'priceTotal') SearchPostalCodePriceEmphasis,
                              D.DrivetimeRank,
                              D.IsClosestFlag,
                              D.DrivetimeMax,
                              TargetOffset,
                              TrueCarDiscountPercent = DA.TrueCarDiscountPercent,
                              DA.HasPreInstall,
                              DA.HasTrueCarDiscount,
                              DA.HasValidAccessories,
                              DA.DealershipAccessoryTotal,
                              DA.DealershipDiscountedAccessoryTotal,
                              DF.HasOtherFees,
                              DF.HasValidFees,
                              DF.DealershipFeeTotal,
                              T.GenericStyle,
                              ShowTransparencyPledge = CASE WHEN P.IsExcluded = 1 THEN CAST(0 AS BIT) ELSE ISNULL(D.ShowTransparencyPledgeNew,0) END
                            FROM
                              dbo.FnGetProgramAffinityGroupBase(@ProgramAffinityGroupId) PAG
                              INNER JOIN
                              mta_postal_code PC WITH (NOLOCK) ON PC.postal_code = @SearchPostalCode
                              CROSS APPLY
                              veh.FnGetTrimBaseByQueryType(@QueryType, @MakeId, NULL, @TrimId) T
                              LEFT OUTER JOIN
                              dbo.tbl_make_model_trim_mercedes_benz_national_offset TNO WITH (NOLOCK) ON T.TrimId = TNO.trim_id
                              CROSS APPLY
                              veh.FnGetStateRules(@ProgramAffinityGroupId, PC.[State], @IsLoggedIn, T.MakeId) SR
                              LEFT OUTER JOIN
                              dbo.tbl_make_open_market_offset_limits OL WITH (NOLOCK) ON T.MakeId = OL.make_id
                              LEFT OUTER JOIN
                              dlr.FnGetDealershipsV3(@ProgramAffinityGroupId, @SearchPostalCode, CASE WHEN @QueryType IN ('All', 'Comparables') THEN NULL ELSE @MakeId END, @distance, @DefaultDealershipId, @AbTests, @IncludeAllDealershipsForLogging, @DealershipIds) D ON T.MakeId = D.MakeId
                              OUTER APPLY
                              veh.FnGetStateRules(@ProgramAffinityGroupId, D.DealershipState, @IsLoggedIn, D.MakeId) DSR
                              LEFT OUTER JOIN
                              dlr.DealershipTrimPricing P WITH (NOLOCK) ON T.TrimId = P.TrimId AND D.DealershipId = P.DealershipId AND T.MakeId = D.MakeId
                              LEFT OUTER JOIN
                              dlr.DealershipTrimPricingIncentive TPI WITH (NOLOCK) ON P.TrimId = TPI.TrimId AND P.DealershipId = TPI.DealershipId AND TPI.ProgramAffinityGroupId = @ProgramAffinityGroupId
                              OUTER APPLY
                              dlr.GetDealershipTrimNationalMemberNetworkOffset(@ProgramAffinityGroupId, T.MakeId, P.MemberNetworkOffset, TNO.tmv_discount, P.IsExcluded, D.DealershipId, PAG.UseNationalMemberNetworkOffset) MNO
                              OUTER APPLY
                              dlr.FnGetDealershipAccessoriesTotal(D.DealershipId, T.GenericStyle, 1, T.MakeId, T.ModelId) DA
                              outer APPLY
                              dlr.FnGetDealershipFeesTotal(D.DealershipId) DF
                          ) V
                      ) U
                      OUTER APPLY
                      util.FnGetPriceExperienceAndIsInvoiceVisibleScoreV2(U.DealershipIsInvoiceVisibleFlag, U.SearchPostalCodeIsInvoiceVisibleFlag, U.DealershipPriceExperience, U.SearchPostalCodePriceExperience, U.IsExcluded) SC
                  ) W
                  CROSS APPLY
                  util.FnGetPriceExperienceAndIsInvoiceVisible(W.ScoreMin, W.ScoreMax) PE
                  LEFT OUTER JOIN
                  dlr.HistoricalAverageMemberOffsetByPostalCodePartial HAMO WITH (nolock) ON W.PricingTrimGroupId = HAMO.PricingTrimGroupId and HAMO.PostalCodePartial = convert(char(3), left(isnull(W.DealershipPostalCode, @SearchPostalCode), 3))
                  OUTER APPLY
                  dlr.FnGetHistoricalTotalOffset(W.TotalOffset, W.OpenNetworkPremium, HAMO.HistoricalAverageMemberOffset, W.EditorialTargetTotalOffset) HTO
                  LEFT OUTER JOIN
                  tcpr.GeoState GS WITH (NOLOCK) on W.SearchState = GS.ShortName
                  LEFT outer JOIN
                  dlr.SupraLocalTargetOffset STO WITH (NOLOCK) ON STO.TrimId = W.TrimId AND STO.SupraLocalId = GS.RegionId
                WHERE
                  @IncludeAllDealershipsForLogging = 1
                  OR DsaVersionId = 2
                  OR CASE WHEN (@BestPriceQuote = 1 AND SortOrder <= 1) or (@BestPriceQuote = 0 and (SortOrder <= 3 OR IsTexasZoneFlag = 1 OR DealershipId IS null)) THEN 1 ELSE 0 END = 1 -- IsDisplayedFlag
              ) X
              OUTER APPLY
              dbo.FnGetTrimColorPricingByIdsV2(@IsUnConfiguredFlag, X.TrimId, @ExteriorColorId, @InteriorColorId, X.[ExteriorColorSticker] + X.[InteriorColorSticker], X.[ExteriorColorInvoice] + X.[InteriorColorInvoice]) C
              OUTER APPLY
              dbo.FnGetTrimOptionPricingBySmallIdsV3(@IsUnConfiguredFlag, X.TrimId, @OptionIds, X.[OptionsSticker], X.[OptionsInvoice], CASE WHEN @InteriorColorId IS NULL AND @ExteriorColorId IS NULL THEN 1 ELSE 0 end) O
              OUTER APPLY
              dbo.FnGetTrimRegionalFeesPricingByIdsV2(@IsUnConfiguredFlag, X.TrimId, X.RegionIds, X.InvoiceBase, X.StickerBase, ISNULL(C.InvoiceColorTotal, 0) + ISNULL(O.InvoiceOptionTotal, 0), ISNULL(C.StickerColorTotal, 0) + ISNULL(O.StickerOptionTotal, 0), X.DestinationFee, X.InvoiceRegionalFees, X.InvoiceOptionsRegionalFees, X.StickerRegionalFees) RAF
              OUTER APPLY
              sav.FnGetTrimIncentiveTotalV2(@IsUnConfiguredFlag, 0, @ProgramAffinityGroupId, X.MakeId, X.TrimId, @SearchPostalCode, @IncludeProgramIncentives, @IncentiveIds, null, null, null, null, null, X.HasFinancing, X.AllowSavingsFinder, X.UseInternalIncentives, null) I
              OUTER apply
              sav.FnGetTrimDealershipIncentiveTotalV2(@IsUnConfiguredFlag, @ProgramAffinityGroupId, X.MakeId, @SearchPostalCode, X.DealershipId, null, @IncentiveIds, X.DealershipToCustomerIncentives, 0, null, X.IsOpenNetworkFlag) DI
              OUTER APPLY
              dbo.FnGetTrimRegionalFeesPricingByPostalCodeV2(CASE WHEN X.DealershipId IS NOT NULL THEN 1 ELSE 0 end, X.TrimId, @SearchPostalCode, InvoiceBase, StickerBase, ISNULL(InvoiceColorTotal, 0) + ISNULL(InvoiceOptionTotal, 0), ISNULL(StickerColorTotal, 0) + ISNULL(StickerOptionTotal, 0), DestinationFee) RAFT
          ) Y
      ) Z
  )

GO


