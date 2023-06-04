from typing import Protocol, List

import numpy as np

from models import BehaviorData


class IFeatureTransformer(Protocol):

    def process(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> np.ndarray:
        """
        transform sub_session to feature vector.
        sub_session is a part of session, session is a whole session.
        Args:
            session: List of BehaviorData within this session
            sub_session: list of BehaviorData within this sub-session
        Returns:
            feature_vector: np.ndarray with shape (feature_dim,)
        """
        ...


class FeatureTransformer(IFeatureTransformer):

    def process(self, sub_session: List[BehaviorData], session: List[BehaviorData]) -> np.ndarray:
        """
        sub_session is a part of session, session is a whole session.
        Args:
            session: List of BehaviorData within this session
            sub_session: list of BehaviorData within this sub-session
        Returns:
            feature_vector: np.ndarray with shape (feature_dim,)
        """
        # here is an example implementation

        # feature 1: session duration
        session_duration = (sub_session[-1].EventTime - sub_session[0].EventTime).total_seconds()

        # feature 2: number of actions
        num_actions = len(sub_session)

        # feature 3: number of unique pages
        num_unique_pages = len(set([action.PageType for action in sub_session if action.PageType is not None]))

        # feature 4: is a member?
        is_member = 0 if session[0].MemberData is None else 1

        # feature 5: App or Web / Which Tunnel
        is_App = 1 if session[0].Tunnel == 'App' else 0

        # feature 6: Which device (1:iOS APP, 2:MobileWeb, 3:Android APP, 4:Desktop)
        device = 1
        if session[0].Device == 'iOS APP':
            device = 1
        elif session[0].Device == 'iOS APP':
            device = 2
        elif session[0].Device == 'Android APP':
            device = 3
        else:
            device = 4

        # feature 7: UnitPrice 可以按照級距分成可能五個級距等等
        #UnitPrice = 1


        # feature 8:  ContentType數量
        content_types = len(set([action.ContentType for action in sub_session if action.ContentType is not None]))

        # feature 9: Banner001數量
        Banner001 = len(set([action.ContentType for action in sub_session if action.ContentType is not None and action.ContentType == 'Banner001']))

        # feature 10: TabBar數量
        TabBar = len(set([action.ContentType for action in sub_session if action.ContentType is not None and action.ContentType == 'TabBar']))

        # feature 11: PopupAD數量
        PopupAD = len(set([action.ContentType for action in sub_session if action.ContentType is not None and action.ContentType == 'PopupAD']))

        # feature 12: PageType中的的Home數量
        Home = len(set([action.PageType for action in sub_session if action.PageType is not None and action.PageType == 'Home']))

        # feature 13: CustomPage數量
        CustomPage = len(set([action.PageType for action in sub_session if action.PageType is not None and action.PageType == 'CustomPage']))

        # feature 14: SideBar數量
        Sidebar = len(set([action.PageType for action in sub_session if action.PageType is not None and action.PageType == 'SideBar']))

        # 心珮
        num_unique_pages = len(set([action.ContentType for action in sub_session if action.ContentType is not None]))

        # feature x: Which UTMSource (0:other, 1:(direct), 2:FB_CA, 3:brand.com, 4:Audience, 5:google)
        UTMSource = 1
        if session[0].UTMSource == '(direct)':
            UTMSource = 1
        elif session[0].UTMSource == 'FB_CA':
            UTMSource = 2
        elif session[0].UTMSource == 'brand.com':
            UTMSource = 3
        elif session[0].UTMSource == 'Audience':
            UTMSource = 4
        elif session[0].UTMSource == 'google':
            UTMSource = 5
        else:
            UTMSource = 0

        # feature x: ContentName (0:other, 1:Home, 2:VipMember, 3:ShoppingCart, 4:dose_0322-0327_LINE購物8%, 5:WishList)
        ContentName = 1
        if session[0].ContentName is None:
            ContentName = 0
        elif session[0].ContentName == 'Home':
            ContentName = 1
        elif session[0].ContentName == 'VipMember':
            ContentName = 2
        elif session[0].ContentName == 'ShoppingCart':
            ContentName = 3
        elif session[0].ContentName == 'dose_0322-0327_LINE購物8%':
            ContentName = 4
        elif session[0].ContentName == 'WishList':
            ContentName = 5
        else:
            ContentName = 0

        # feature 5: number of each Behavior type
        #viewcategory, viewactivity, viewalbumdetail, viewvideodetail, viewarticledetail, viewecoupondetail,
        #viewpromotiondetail, viewproduct, search, addToCart, checkout
        viewcategory_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewcategory'])
        viewactivity_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewactivity'])
        viewalbumdetail_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewalbumdetail'])
        viewvideodetail_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewvideodetail'])
        viewarticledetail_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewarticledetail'])
        viewecoupondetail_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewecoupondetail'])
        viewpromotiondetail_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewpromotiondetail'])
        viewproduct_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewproduct'])
        search_count = len([action.Behavior for action in sub_session if action.Behavior == 'search'])
        addtocart_count = len([action.Behavior for action in sub_session if action.Behavior == 'addtocart'])
        viewmainpage_count = len([action.Behavior for action in sub_session if action.Behavior == 'viewmainpage'])

        # feature 6: total time of each Behavior type ######need to add total time column######
        viewcategory_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewcategory'])
        viewactivity_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewactivity'])
        viewalbumdetail_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewalbumdetail'])
        viewvideodetail_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewvideodetail'])
        viewarticledetail_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewarticledetail'])
        viewecoupondetail_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewecoupondetail'])
        viewpromotiondetail_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewpromotiondetail'])
        viewproduct_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewproduct'])
        search_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'search'])
        addtocart_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'addtocart'])
        viewmainpage_time = sum([(action2.EventTime - action1.EventTime).total_seconds() for action1, action2 in zip(sub_session[:-1], sub_session[1:]) if action1.Behavior == 'viewmainpage'])

        return np.array([
            session_duration,
            num_actions,
            num_unique_pages,
            is_member,
            is_App,
            device,
            content_types,
            Banner001,
            TabBar,
            PopupAD,
            Home,
            CustomPage,
            Sidebar,
            num_unique_pages,
            UTMSource,
            ContentName,
            viewcategory_count,
            viewactivity_count,
            viewalbumdetail_count,
            viewvideodetail_count,
            viewarticledetail_count,
            viewecoupondetail_count,
            viewpromotiondetail_count,
            viewproduct_count,
            search_count,
            addtocart_count,
            viewmainpage_count,
            viewcategory_time,
            viewactivity_time,
            viewalbumdetail_time,
            viewvideodetail_time,
            viewarticledetail_time,
            viewecoupondetail_time,
            viewpromotiondetail_time,
            viewproduct_time,
            search_time,
            addtocart_time,
            viewmainpage_time
        ])
