// fred 1

package com.medline.parscan.ParScanResidents;
import java.awt.Color;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URLEncoder;
import java.rmi.server.UID;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.PropertyResourceBundle;
import java.util.Random;
import java.util.ResourceBundle;

import javax.ejb.CreateException;
import javax.ejb.MessageDrivenContext;
import javax.ejb.SessionBean;
import javax.ejb.SessionContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.naming.NamingException;
import javax.resource.ResourceException;
import javax.resource.cci.MappedRecord;
import javax.resource.cci.RecordFactory;
import javax.sql.DataSource;

import org.apache.commons.net.ftp.FTPClient;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Months;
import org.joda.time.Weeks;
import org.joda.time.Years;

import ATGOrderProxy.ManageOrderService;
import ATGOrderProxy.ManageOrderServiceService;
import ATGOrderProxy.types.p1.AuthenticationDetails;
import ATGOrderProxy.types.p1.LineItem;
import ATGOrderProxy.types.p1.ManageOrder;
import ATGOrderProxy.types.p1.ManageOrderRequestWrapper;
import ATGOrderProxy.types.p3.ManageOrderRequest;

import com.lowagie.text.Chunk;
import com.lowagie.text.Document;
import com.lowagie.text.DocumentException;
import com.lowagie.text.Element;
import com.lowagie.text.Font;
import com.lowagie.text.FontFactory;
import com.lowagie.text.HeaderFooter;
import com.lowagie.text.Image;
import com.lowagie.text.PageSize;
import com.lowagie.text.Paragraph;
import com.lowagie.text.Phrase;
import com.lowagie.text.pdf.PdfPCell;
import com.lowagie.text.pdf.PdfPTable;
import com.lowagie.text.pdf.PdfWriter;
import com.medline.common.json.JSONArray;
import com.medline.common.json.JSONObject;
import com.medline.crm.common.StringUtils;
import com.medline.esa.customer.proxy.ZgetCustomersbyUserAlias;
import com.medline.esa.customer.proxy.ZgetCustomersbyUserAliasService;
import com.medline.esa.customer.proxy.types.UserData;
import com.medline.packaging.proxy.ZgetProductUnitofMeasures;
import com.medline.packaging.proxy.ZgetProductUnitofMeasuresService;
import com.medline.packaging.proxy.types.ProductUOMRequest;
import com.medline.packaging.proxy.types.ProductUnitofMeasuresDT;
import com.medline.parscan.ParScanResidents.Bean.Customer;
import com.medline.parscan.ParScanResidents.Bean.GtinData;
import com.medline.parscan.ParScanResidents.Bean.ItemBillUomBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanCategoryBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanDeviceBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanEmployeeBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanEmployeeScanBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanFillUpBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanItemBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanPOBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanParAreaBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanPayorCodeBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanPropertyBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanReportMessage;
import com.medline.parscan.ParScanResidents.Bean.ParScanResidentBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanResidentChargeBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanResidentChargeDetailBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanServiceBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanStockBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanUOMBean;
import com.medline.parscan.ParScanResidents.Bean.ParScanVendorBean;
import com.medline.parscan.ParScanResidents.Bean.ReportCriteria;
import com.medline.parscan.ParScanResidents.Bean.SyncData;
import com.medline.parscan.ParScanResidents.Bean.UpdateGtinRequest;
import com.medline.parscan.ParScanResidents.Bean.UpdateGtinResponse;
import com.medline.parscan.ParScanResidents.DssiConnector.DssiConnector;
import com.medline.parscan.ParScanResidents.DssiConnector.DssiConnectorHttpClient;
import com.medline.parscan.ParScanResidents.Exception.ParScanResidentsEJBException;
import com.medline.parscan.ParScanResidents.Exception.SimilarResidentExistException;
import com.medline.parscan.ParScanResidents.ParScan.config.ParScanConfigJndi;
import com.medline.parscan.ordertemplate.GetOrderTemplatesService;
import com.medline.parscan.ordertemplate.GetOrderTemplatesServiceService;
import com.medline.parscan.ordertemplate.types.p1.GetOrderTemplatesRequest;
import com.medline.parscan.ordertemplate.types.p1.GetOrderTemplatesResponse;
import com.medline.parscan.ordertemplate.types.p1.OrderTemplate;
import com.medline.parscan.ordertemplate.types.p1.OrderTemplateCategory;
import com.medline.parscan.ordertemplate.types.p1.TemplateItem;
import com.medline.services.IApplicationEmail;
import com.medline.services.bean.email.IApplicationEmailItem;
import com.medline.services.exception.ApplicationEmailException;
import com.medline.srp.Cust.CustomerSearchSSLocal;
import com.medline.srp.Cust.CustomerSearchSSLocalHome;
import com.medline.srp.Cust.bean.CustomerDetailsBean;
import com.medline.srp.Cust.bean.CustomerFunctionsBean;
import com.medline.srp.common.KMUtils;
import com.medline.srp.pricing.bean.price.PricingDetailsBean;
import com.medline.srp.pricing.bean.session.PricingSSLocal;
import com.medline.srp.pricing.bean.session.PricingSSLocalHome;
import com.medline.srp.product.beans.UnitOfMeasure;
import com.medline.srp.product.beans.packaging.PackagingDetails;
import com.sap.engine.services.configuration.appconfiguration.ApplicationConfigHandlerFactory;
import com.sap.portal.connectivity.destinations.DestinationFilter;
import com.sap.portal.connectivity.destinations.IDestinationWrapper;
import com.sap.portal.connectivity.destinations.IDestinationsService;
import com.sap.portal.connectivity.destinations.PortalDestinationsServiceException;
import com.sap.security.api.IGroup;
import com.sap.security.api.IUser;
import com.sap.security.api.UMFactory;
import com.sap.tc.logging.Location;
import com.sapportals.connector.connection.IConnection;
import com.sapportals.connector.execution.functions.IInteraction;
import com.sapportals.connector.execution.functions.IInteractionSpec;
import com.sapportals.connector.execution.structures.IRecordSet;
import com.sapportals.connector.execution.structures.IStructureFactory;
import com.sapportals.connector.metadata.functions.IFunction;
import com.sapportals.connector.metadata.functions.IFunctionsMetaData;
import com.sapportals.portal.prt.jndisupport.InitialContext;
import com.sapportals.portal.prt.runtime.PortalRuntime;
import com.sapportals.portal.security.usermanagement.UserManagementException;
import com.sapportals.wcm.repository.AccessDeniedException;
import com.sapportals.wcm.repository.Content;
import com.sapportals.wcm.repository.ICollection;
import com.sapportals.wcm.repository.IProperty;
import com.sapportals.wcm.repository.IResource;
import com.sapportals.wcm.repository.IResourceContext;
import com.sapportals.wcm.repository.PropertyName;
import com.sapportals.wcm.repository.ResourceContext;
import com.sapportals.wcm.repository.ResourceFactory;
import com.sapportals.wcm.util.content.IContent;
import com.sapportals.wcm.util.uri.RID;
import com.sapportals.wcm.util.usermanagement.WPUMFactory;

// fred 2

/**
 * @ejbHome <{com.medline.parscan.ParScanResidents.ParScanResidentsSSHome}>
 * @ejbLocal <{com.medline.parscan.ParScanResidents.ParScanResidentsSSLocal}>
 * @ejbLocalHome <{com.medline.parscan.ParScanResidents.ParScanResidentsSSLocalHome}>
 * @ejbRemote <{com.medline.parscan.ParScanResidents.ParScanResidentsSS}>
 * @stateless 
 * @transactionType Container
 */
public class ParScanResidentsSSBean implements SessionBean 
{
	private transient MessageDrivenContext mdc = null;
	private static final String TRANSFERED_FROM = "Transferred from ";
	private static final String TRANSFERED_TO	= "Transferred to ";
	private static final String FORMULARY_YES_FLAG = "Y";
	private static final Location  logger = Location.getLocation(ParScanResidentsSSBean.class);
	private static final ResourceBundle sqlProp = PropertyResourceBundle.getBundle("com.medline.parscan.ParScanResidents.parScanResidentSql", new Locale(""));
	static final Hashtable environment = new Hashtable();
	
	static 
	{
		environment.put(
			"java.naming.factory.initial",
			"com.sapportals.portal.prt.registry.PortalRegistryFactory");
	}

	public void ejbRemove() {
	}

	public void ejbActivate() {
	}

	public void ejbPassivate() {
	}

	public void setSessionContext(SessionContext context) {
		myContext = context;
	}

	private SessionContext myContext;
	/**
	 * Create Method.
	 */
	public void ejbCreate() throws CreateException {
		// TODO : Implement
	}

	private Connection openConnection() throws Exception 
	{
		Connection conn = null;

		InitialContext ctx = new InitialContext();
		DataSource ds = (DataSource) ctx.lookup("jdbc/SAP/BC_JMS");		
		conn = ds.getConnection();

		return conn;
	}

	private IConnection getCRMConnection() throws NamingException, PortalDestinationsServiceException 
	{
		//get the user
		IUser user = UMFactory.getAuthenticator().getLoggedInUser();
		//get a connection
		InitialContext context =
			new InitialContext(ParScanResidentsSSBean.environment);

		IDestinationsService destinationsService =
			(IDestinationsService) context.lookup(
				IDestinationsService.SERVICE_JNDI_NAME);

		// define a destination filter to restrict to the RFC defined destinations
		DestinationFilter destinationFilter1 =
			new DestinationFilter(
				DestinationFilter.SOURCE_PORTAL_SYSTEM_LANDSCAPE_SERVICE,
				DestinationFilter.TYPE_SAP);

		IDestinationWrapper destinationWrapper =
			destinationsService.getDestinationWrapper(user, "CRM");

		return destinationsService.getConnection(user, "CRM");
	}

	private IConnection getR3Connection() throws NamingException, PortalDestinationsServiceException 
	{
		//get the user
		IUser user = UMFactory.getAuthenticator().getLoggedInUser();

		//get a connection
		InitialContext context =
			new InitialContext(ParScanResidentsSSBean.environment);

		IDestinationsService destinationsService =
			(IDestinationsService) context.lookup(
				IDestinationsService.SERVICE_JNDI_NAME);

		// define a destination filter to restrict to the RFC defined destinations
		DestinationFilter destinationFilter1 =
			new DestinationFilter(
				DestinationFilter.SOURCE_PORTAL_SYSTEM_LANDSCAPE_SERVICE,
				DestinationFilter.TYPE_SAP);

		IDestinationWrapper destinationWrapper =
			destinationsService.getDestinationWrapper(user, "ECC");
		if (destinationWrapper.hintAreCredentialsSet(user)) 
		{
			// has mapped user so use it
			return destinationsService.getConnection(user, "ECC");
		} 
		else 
		{
			//use SSO
			return destinationsService.getConnection(user, "ECCSSO");
		}
	}

	private Map getResident(String customer) throws ParScanResidentsEJBException 
	{
		IConnection connection = null;
		SimpleDateFormat sdfSource = new SimpleDateFormat("yyyy-MM-dd");		
		Map keyMap = new HashMap();
		
		try 
		{
			boolean activeResidents = false;
			List resList = getAllResidents( customer, activeResidents);

			for(Iterator itor = resList.iterator();itor.hasNext();)
			{
				ParScanResidentBean data = (ParScanResidentBean) itor.next();
				
				keyMap.put(data.getCrmGUID(), data);
			}

		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			
			ParScanResidentsSSBean.logger.errorT(
				"Error during getResident: " + stringWriter.toString());
			
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (connection != null)
					connection.close();
			} 
			catch (ResourceException ex) 
			{
			}
		}
		return keyMap;
	}
	
	
	// fred 4
	private List getAllResidents( String customer, boolean activeResidents ) throws ParScanResidentsEJBException
	{
		ParScanResidentBean searchCriteria = new ParScanResidentBean();
		searchCriteria.setAllBlank();
		searchCriteria.setCustomer( customer );
		
		return getResidentList( searchCriteria, activeResidents ? "X" : "" );

	}

	/**
	 * Business Method.
	 */
	private ParScanResidentBean getGUIDResident(String customerNumber, String GUID)throws ParScanResidentsEJBException 
	{
		String methodName = "getGUIDResident()";
		logger.entering( methodName );
		IConnection connection = null;
		SimpleDateFormat sdfSource = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		ParScanResidentBean data = new ParScanResidentBean();
		
		try 
		{
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "ZV_PATIENT_SEARCH");

			IFunctionsMetaData functionsMetaData = connection.getFunctionsMetaData();
			IFunction function = functionsMetaData.getFunction("ZV_PATIENT_SEARCH");

			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams = recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory = interaction.retrieveStructureFactory();

			importParams.put("I_SOLDTO", customerNumber);
			importParams.put("I_GUID", GUID);

			MappedRecord exportParams = (MappedRecord) interaction.execute( interactionSpec, importParams );
			IRecordSet residents = (IRecordSet) exportParams.get("T_PATIENT_DATA");

			while (residents.next()) 
			{		
				data.setCrmGUID(residents.getString("GUID"));
				data.setId(residents.getString("PATIENT_ID"));
				data.setFirstName(residents.getString("PFNAME"));
				data.setLastName(residents.getString("PLNAME"));
				data.setRoom(residents.getString("ROOM_NUMBER"));
				data.setFloor(residents.getString("FLOOR"));
				data.setWing(residents.getString("WING"));
				data.setCategory(residents.getString("CARE_TYPE"));
				if( isValidDate( residents.getString("ADMIT_DATE") ))
				{
					Date date = sdfSource.parse(residents.getString("ADMIT_DATE"));				
					data.setAdmitDate(dateFormat.format(date));
				}
				else
				{
					data.setAdmitDate("");
				}
				if(isValidDate( residents.getString("DISMISS_DATE") ))
				{
					Date date = sdfSource.parse(residents.getString("DISMISS_DATE"));				
					data.setDismissDate(dateFormat.format(date));
				}
				else
				{
					data.setDismissDate("");
				}
										
			}

		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT( "Error during getGUIDResident: " + stringWriter.toString());
			
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (connection != null)
					connection.close();
					
					logger.exiting( methodName );
			} 
			catch (ResourceException ex) 
			{
				logger.errorT(" Failed to close CRM connection, " + ex.toString() );
			}
		}
		
		return data;
	}

	
	// fred 5
	/**
	 * Business Method.
	 */
	private ParScanResidentBean getResident(String customerNumber, String ID, String Name, String wing, String floor, String room, String careType, String active)
	throws ParScanResidentsEJBException 
	{
		IConnection connection = null;
		SimpleDateFormat sdfSource = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		ParScanResidentBean data = new ParScanResidentBean();
		
		try 
		{
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "ZV_PATIENT_SEARCH");

			IFunctionsMetaData functionsMetaData = 	connection.getFunctionsMetaData();
			IFunction function = functionsMetaData.getFunction("ZV_PATIENT_SEARCH");

			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams = recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory = interaction.retrieveStructureFactory();

			importParams.put("I_SOLDTO", customerNumber);
			importParams.put("I_PATIENT_ID", ID);
			importParams.put("I_PNAME", Name);
			importParams.put("I_WING", wing);
			importParams.put("I_FLOOR", floor);
			importParams.put("I_ROOM_NUMBER", room);
			importParams.put("I_CARE_TYPE", careType);
			importParams.put("I_ACTIVE", active);

			MappedRecord exportParams = (MappedRecord) interaction.execute( interactionSpec, importParams);
			IRecordSet residents = (IRecordSet) exportParams.get("T_PATIENT_DATA");

			while (residents.next()) 
			{		
				data.setCrmGUID(residents.getString("GUID"));
				data.setId(residents.getString("PATIENT_ID"));
				data.setFirstName(residents.getString("PFNAME"));
				data.setLastName(residents.getString("PLNAME"));
				data.setRoom(residents.getString("ROOM_NUMBER"));
				data.setFloor(residents.getString("FLOOR"));
				data.setWing(residents.getString("WING"));
				data.setCategory(residents.getString("CARE_TYPE"));
				
				if( isValidDate( residents.getString("ADMIT_DATE") ))
				{
					Date date = sdfSource.parse(residents.getString("ADMIT_DATE"));				
					data.setAdmitDate(dateFormat.format(date));
				}
				else
				{
					data.setAdmitDate("");
				}
				if( isValidDate( residents.getString("DISMISS_DATE") ) )
				{
					Date date = sdfSource.parse(residents.getString("DISMISS_DATE"));				
					data.setDismissDate(dateFormat.format(date));
				}
				else
				{
					data.setDismissDate("");	
				}				
			}

		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT( "Error during getResident: " + stringWriter.toString());
			
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (connection != null)
					connection.close();
			} 
			catch (ResourceException ex) 
			{
				logger.errorT(" Failed to close CRM connection, " + ex.toString() );
			}
		}
		
		return data;
	}
		
	// fred 6
	public String getActiveAndDismissedResident( ParScanResidentBean residentSearchParams, String fromDate, String toDate, String billExclusion )  
	throws ParScanResidentsEJBException
	{
		String methodName = "getActiveAndDismissedResident()";
		logger.entering( methodName );
		
		StringBuffer sb = new StringBuffer();
		int counter = 0;
		
		try
		{
			List residentList = new ArrayList();
			String dismissDate = "";
			String residentGUID = "";
			String customer = "";
					
			List residentActiveAndDismissedList = getResidentList( residentSearchParams, "" );
						
			for( Iterator itor = residentActiveAndDismissedList.iterator(); itor.hasNext(); )
			{
				ParScanResidentBean data = ( ParScanResidentBean ) itor.next();
				dismissDate = data.getDismissDate();
				residentGUID = data.getCrmGUID();
				customer = residentSearchParams.getCustomer();
				
				boolean isDissmissDateValid = false;
				if(StringUtils.isNotEmpty(dismissDate)) 
				{
					try 
					{
						SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yyyy" );
						Date startDate = sdf.parse( fromDate );
						Date dismissedDate = sdf.parse( dismissDate );
						isDissmissDateValid = dismissedDate.after(startDate) || dismissedDate.equals(startDate);
					}
					catch (ParseException e) 
					{
						logger.debugT("Exception occurred while parsing date");
					}						
				}
			
				if ( StringUtils.isEmpty( dismissDate ) || isDissmissDateValid ) 
				{
					residentList.add(data);
				}
			}
			logger.debugT("ResidentActiveAndDismissedList List size :: " + residentActiveAndDismissedList.size());
			logger.debugT("Filtered Resident List size :: " + residentList.size());
			
			sb.append("[");
			for( Iterator itor = residentList.iterator(); itor.hasNext(); )
			{
				counter++;
				ParScanResidentBean data = (ParScanResidentBean) itor.next();
				if (counter > 1)
				{
					sb.append(",");
				}
										
				sb.append("{");
				sb.append("guid: '" + StringUtils.escapeChars(data.getCrmGUID()) + "',");
				sb.append("id: '" + StringUtils.escapeChars(data.getId()) + "',");
				sb.append("firstname: '" + StringUtils.escapeChars(data.getFirstName()) + "',");
				sb.append("lastname: '" + StringUtils.escapeChars(data.getLastName()) + "',");
				sb.append("room: '" + StringUtils.escapeChars(data.getRoom()) + "',");
				sb.append("floor: '" + StringUtils.escapeChars(data.getFloor()) + "',");
				sb.append("wing: '" + StringUtils.escapeChars(data.getWing()) + "',");
				sb.append("type: '" + StringUtils.escapeChars(data.getCategory()) + "',");
				sb.append("admit: '" + StringUtils.escapeChars(data.getAdmitDate()) + "',");
				sb.append("dismiss: '" + StringUtils.escapeChars(data.getDismissDate()) + "'");			
				sb.append("}");
			}
			sb.append("]");
		}
		catch (Exception e)
		{
			StringWriter stringWriter = new StringWriter();
			e.printStackTrace( new PrintWriter( stringWriter ) );
			logger.errorT( "Error during getActiveAndDismissedResident() : " + stringWriter.toString() );
			
			throw new ParScanResidentsEJBException(e);
		}
		finally
		{
			logger.exiting( methodName );
			return sb.toString();
		}
	}

	private boolean isDateInRange( String fromDate, String toDate, String dismissDate ) throws ParScanResidentsEJBException
	{
		try
		{
			SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yyyy" );
			Date startDate = sdf.parse( fromDate );
			Date endDate = sdf.parse( toDate );
			Date inDate	= sdf.parse( dismissDate );	
			
			if ( ( inDate.compareTo(startDate) >= 0 ) && ( inDate.compareTo(endDate) <= 0 ) )
				return true;
		}
		catch (Exception e)
		{
			StringWriter stringWriter = new StringWriter();
			e.printStackTrace( new PrintWriter( stringWriter ) );
			logger.errorT( "Error during isDateInRange() : " + stringWriter.toString() );
			
			throw new ParScanResidentsEJBException(e);
		}
		return false;
	}
		
	private boolean isDismissedResidentHasCharge( String residentGUID, String customer, String fromDate, String toDate, String billExclusion )
	throws ParScanResidentsEJBException
	{
		String methodName = "isDismissedResidentHasCharge()";
		logger.entering(methodName);
		
		try
		{
			SimpleDateFormat sdf = new SimpleDateFormat( "MM/dd/yyyy" );
			Date startDate = sdf.parse( fromDate );
			Date endDate = sdf.parse( toDate );
			
			List residentHasBill = getResidentBill( residentGUID, customer, startDate, endDate, "", "", billExclusion );
			
			if ( residentHasBill.size() > 0 )
				return true;
		}
		catch (Exception e)
		{
			StringWriter stringWriter = new StringWriter();
			e.printStackTrace( new PrintWriter( stringWriter ) );
			logger.errorT( "Error during isDismissedResidentHasCharge() : " + stringWriter.toString() );
			
			throw new ParScanResidentsEJBException(e);
		}
		
		logger.exiting(methodName);
		return false;
	}
	
	/**
	 * Business Method.
	 */
	public List getResidentList( ParScanResidentBean searchDetails, String active) throws ParScanResidentsEJBException 
	{
		String methodName = "getResidentList()";
		logger.entering( methodName );
	
		List residentList = new ArrayList();
		IConnection connection = null;
		SimpleDateFormat sdfSource = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		String uiDateFormat = "MM/dd/yyyy";
		
		logger.debugT( " searchDetails = \n" + searchDetails);
		
		try 
		{
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "ZV_PATIENT_SEARCH");

			IFunctionsMetaData functionsMetaData = connection.getFunctionsMetaData();
			IFunction function = functionsMetaData.getFunction("ZV_PATIENT_SEARCH");

			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams = recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory = interaction.retrieveStructureFactory();
			
			String admitDate = isValidDate( searchDetails.getAdmitDate() ) ? convertToyyyyMMdd( searchDetails.getAdmitDate(), uiDateFormat ) : searchDetails.getAdmitDate();
			String dismissDate = isValidDate( searchDetails.getDismissDate() ) ? convertToyyyyMMdd( searchDetails.getDismissDate(), uiDateFormat ) : searchDetails.getDismissDate();

			importParams.put("I_SOLDTO", searchDetails.getCustomer());
			importParams.put("I_PATIENT_ID", searchDetails.getId());
			importParams.put("I_PNAME", searchDetails.getName());
			importParams.put("I_WING", searchDetails.getWing());
			importParams.put("I_FLOOR", searchDetails.getFloor());
			importParams.put("I_ROOM_NUMBER", searchDetails.getRoom());
			importParams.put("I_CARE_TYPE", searchDetails.getCategory());
			
			importParams.put("I_CR_DATE_FROM", admitDate );
			importParams.put("I_CR_DATE_TO", dismissDate );
			importParams.put("I_ACTIVE", active);

			MappedRecord exportParams = (MappedRecord) interaction.execute( interactionSpec, importParams);
			IRecordSet residents = (IRecordSet) exportParams.get("T_PATIENT_DATA");

			while (residents.next()) 
			{
				ParScanResidentBean data = new ParScanResidentBean();

				data.setCrmGUID(residents.getString("GUID"));
				data.setId(residents.getString("PATIENT_ID"));
				data.setFirstName(residents.getString("PFNAME"));
				data.setLastName(residents.getString("PLNAME"));
				data.setRoom(residents.getString("ROOM_NUMBER"));
				data.setFloor(residents.getString("FLOOR"));
				data.setWing(residents.getString("WING"));
				data.setCategory(residents.getString("CARE_TYPE"));
				
				if( isValidDate( residents.getString("ADMIT_DATE") ) )
				{
					Date date = sdfSource.parse(residents.getString("ADMIT_DATE"));				
					data.setAdmitDate(dateFormat.format(date));
				}
				else
				{
					data.setAdmitDate("");
				}
				
				if( isValidDate( residents.getString("DISMISS_DATE") ) )
				{
					Date date = sdfSource.parse(residents.getString("DISMISS_DATE"));				
					data.setDismissDate(dateFormat.format(date));
				}
				else
				{
					data.setDismissDate("");
				}
				
				residentList.add(data);
			}

		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace( new PrintWriter( stringWriter ) );
			logger.errorT( "Error during getResidentList: " + stringWriter.toString() );
			
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (connection != null)
					connection.close();
			} 
			catch (ResourceException ex) 
			{
				logger.errorT(" Failed to close CRM connection, " + ex.toString() );
			}
			
			logger.exiting( methodName );
		}
		return residentList;
	}

	/**
	 * Business Method.
	 */
	public List getPayorCodes(String customerNumber, String guid)
	throws ParScanResidentsEJBException 
	{
		List PayorCodeList = new ArrayList();
		PreparedStatement stmt;
		Connection conn = null;
		String PayorCode = "";

		try 
		{
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ?");
			stmt.setString(1, customerNumber);
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()) 
			{
				ParScanPayorCodeBean newData = new ParScanPayorCodeBean();
				newData.setPayorCode(rs.getString("PAYOR_CODE"));
				newData.setPayorDescription(rs.getString("DESCRIPTION"));
				newData.setNotes(rs.getString("NOTES"));
				
				PayorCodeList.add(newData);					
			}
			stmt.close();
			
			return PayorCodeList;
		}
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getPayorCodes: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getResidentExpandedBill(String residentGUID,String customer,Date startDate,Date endDate, String sortField, String sortDirection, String billExclusion)
	throws ParScanResidentsEJBException 
	{
		List chargeList = new ArrayList();
		boolean desc = false;
		
		ReportCriteria reportCriteria = new ReportCriteria();
		reportCriteria.residentGUID = residentGUID;
		reportCriteria.customer = customer;
		reportCriteria.startDate = startDate;
		reportCriteria.endDate = endDate;
		reportCriteria.sortField = sortField;
		reportCriteria.sortDirection = sortDirection;
		reportCriteria.billExclusion = billExclusion;
		reportCriteria.allCategories = true;
		try 
		{
			chargeList.addAll(getStandardCharges(reportCriteria));
			chargeList.addAll(getExpandedRecurringCharges(reportCriteria));

			if(!sortDirection.equalsIgnoreCase("ASC"))
				desc = true;
				
			Collections.sort(chargeList, new ChargeComparator(sortField, desc));

			return chargeList;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getResidentExpandedBill: "
					+ stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
	}

	public List getResidentBillForCategory(ReportCriteria reportCriteria)throws ParScanResidentsEJBException
	{
		String methodName = "getResidentBillForCategory()";
		logger.entering( methodName );
		
		List chargeList = new ArrayList();
		boolean desc = false;
		
		try 
		{
			chargeList.addAll(getStandardCharges(reportCriteria));
			chargeList.addAll(getRecurringCharges(reportCriteria));

			if(!reportCriteria.sortDirection.equalsIgnoreCase("ASC"))
				desc = true;
		
			Collections.sort(chargeList, new ChargeComparator(reportCriteria.sortField, desc));
	
			logger.exiting( methodName );
			
			return chargeList;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			
			ParScanResidentsSSBean.logger.errorT("Error during getResidentBill: " + stringWriter.toString());
			
			throw new ParScanResidentsEJBException(ex);
		} 
	}
		
	/**
	 * Business Method.
	 */
	public List getResidentBill(String residentGUID,String customer,Date startDate,Date endDate, String sortField, String sortDirection, String billExclusion)
	throws ParScanResidentsEJBException 
	{	
		String methodName = "getResidentBill()";
		logger.entering(methodName);
		
		ReportCriteria reportCriteria = new ReportCriteria();
		reportCriteria.residentGUID = residentGUID;
		reportCriteria.customer = customer;
		reportCriteria.startDate = startDate;
		reportCriteria.endDate = endDate;
		reportCriteria.sortField = sortField;
		reportCriteria.sortDirection = sortDirection;
		reportCriteria.billExclusion = billExclusion;
		reportCriteria.allCategories = true;
		
		List chargeList = getResidentBillForCategory(reportCriteria);
		
		logger.exiting(methodName);
		
		return chargeList;
	}

	private List getStandardCharges(ReportCriteria reportCriteria)throws ParScanResidentsEJBException
	{
		String methodName = "getStandardCharges()";
		logger.entering(methodName);
		
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;
		PreparedStatement stmtService;
		PreparedStatement stmtProduct;
		DecimalFormat outNumberFormat = new DecimalFormat("0.00");
		Connection conn = null;
		double qty = 0;
		double price = 0;		
		boolean excludeFlag = false;
		boolean chargeFound = false;
		List chargeList = new ArrayList();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy h:mm:ss aa");
		String query = null;
		
		try 
		{
			Timestamp sqlStartDate = new Timestamp(reportCriteria.startDate.getTime());						
			conn = openConnection();
			DateTime eDate = new DateTime(reportCriteria.endDate.getTime());
			eDate = eDate.plusDays(1);			
			Timestamp sqlEndDate = new Timestamp(new java.util.Date(eDate.getMillis()).getTime());

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE RESIDENT_GUID = ? AND CUSTOMER = ? AND CHARGE_DATE BETWEEN ? AND ? ORDER BY CHARGE_ID ASC");
			stmt.setString(1, reportCriteria.residentGUID);
			stmt.setString(2, reportCriteria.customer);
			stmt.setTimestamp(3,sqlStartDate);
			stmt.setTimestamp(4,sqlEndDate);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) 
			{
				excludeFlag = false;
				chargeFound = false;
				
				if(rs.getString("RECURRING_FLAG") == null)
				{
					ParScanResidentChargeBean data = new ParScanResidentChargeBean();

					qty = Double.parseDouble(rs.getString("QUANTITY"));
					price = Double.parseDouble(rs.getString("PRICE"));					
					if (rs.getString("SERVICE_FLAG") != null) 
					{
						//Get service data
						query = reportCriteria.allCategories ? sqlProp.getString( "selectServiceCharge") : 
														sqlProp.getString( "selectServiceChargeForCategory").replaceFirst("#productCategoryCriteria#", reportCriteria.getCategorySqlCriteria());
						logger.debugT("StandardCharges: service charge query =" + query);
						
						stmtService = conn.prepareStatement( query );
								
						stmtService.setString(1, rs.getString("CHARGE_ID"));
						stmtService.setString(2, reportCriteria.customer);

						ResultSet rsService = stmtService.executeQuery();
						if(rsService.next())
						{
							chargeFound = true;
							data.setServiceFlag("X");
							data.setCharge(rsService.getString("SERVICE"));
							data.setAltBarcode(rsService.getString("SERVICE"));
							data.setDescription(rsService.getString("DESCRIPTION"));
							data.setUom("");					
							data.setVendor("");
							data.setVendorItem("");
							data.setVendorUOM("");
							data.setCasePackaging("");
							data.setCost("");
							data.setProductCategory("");
							data.setParArea("");
							data.setPayorType("");
	
							if(rsService.getString("CATEGORY_ID") != null)
							{
								stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
								stmtCategory.setString(1, reportCriteria.customer);
								stmtCategory.setString(2, rsService.getString("CATEGORY_ID"));
								ResultSet rsCategory = stmtCategory.executeQuery();
								if(rsCategory.next())
								{								
									data.setProductCategory(rsCategory.getString("PRODUCT_CATEGORY"));								
	
									if(rsCategory.getString("AR_CODE") == null)
										data.setArCode("");
									else
										data.setArCode(rsCategory.getString("AR_CODE"));									
	
									if(rsCategory.getString("OVERRIDE_FLAG") != null)
									{
										if(data.getArCode().equalsIgnoreCase(""))
											data.setPayorType(data.getProductCategory() + " - " + data.getProductCategory());
										else
											data.setPayorType(data.getProductCategory() + " - " + data.getArCode());
										data.setArCode("");
									}
										
									if(rsCategory.getString("EXCLUDE_FLAG") != null)
										excludeFlag = true;	
								}								
								else
								{
									data.setProductCategory("");
									data.setArCode("");								
								}								
									
								stmtCategory.close();															
							}
							else
							{
								data.setProductCategory("");
								data.setArCode("");							
							}
						}
						
						stmtService.close();
					}
					else 
					{
						//Get product data
						query = reportCriteria.allCategories ? sqlProp.getString( "selectProductCharge") : 
														sqlProp.getString( "selectProductChargeForCategory").replaceFirst("#productCategoryCriteria#", reportCriteria.getCategorySqlCriteria());
						logger.debugT("StandardCharges: product charge query =" + query);
						
						stmtProduct = conn.prepareStatement( query );

						stmtProduct.setString(1, rs.getString("CHARGE_ID"));
						stmtProduct.setString(2, reportCriteria.customer);

						ResultSet rsProduct = stmtProduct.executeQuery();
						if(rsProduct.next())
						{
							chargeFound = true;
							
							data.setServiceFlag("");
							data.setPayorType("");
							data.setCharge(rsProduct.getString("ITEM_ID"));
							data.setDescription(rsProduct.getString("DESCRIPTION"));
							data.setUom(rsProduct.getString("BILL_UOM"));						
							data.setVendorItem(rsProduct.getString("VENDOR_ITEM"));
							data.setVendorUOM(rsProduct.getString("VENDOR_UOM"));
							data.setCasePackaging(rsProduct.getString("CASE_PACKAGING"));
							data.setCost(rsProduct.getString("CURRENT_COST"));
							
							if(rsProduct.getString("ALTERNATE_BARCODE") == null)
								data.setAltBarcode("");
							else
								data.setAltBarcode(rsProduct.getString("ALTERNATE_BARCODE"));						
							
							if(rsProduct.getString("VENDOR_ID") != null)
							{
								stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
								stmtVendor.setString(1, reportCriteria.customer);
								stmtVendor.setString(2, rsProduct.getString("VENDOR_ID"));
								ResultSet rsVendor = stmtVendor.executeQuery();
								if(rsVendor.next())
									data.setVendor(rsVendor.getString("VENDOR_NAME"));
								else							
									data.setVendor("");
									
								stmtVendor.close();	
							}
							else
							{
								data.setVendor("");	
							}
							if(rsProduct.getString("CATEGORY_ID") != null)
							{
								stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
								stmtCategory.setString(1, reportCriteria.customer);
								stmtCategory.setString(2, rsProduct.getString("CATEGORY_ID"));
								ResultSet rsCategory = stmtCategory.executeQuery();
								if(rsCategory.next())
								{
									data.setProductCategory(rsCategory.getString("PRODUCT_CATEGORY"));
									if(rsCategory.getString("AR_CODE") == null)
										data.setArCode("");
									else
										data.setArCode(rsCategory.getString("AR_CODE"));
										
									if(rsCategory.getString("OVERRIDE_FLAG") != null)
									{
										if(data.getArCode().equalsIgnoreCase(""))
											data.setPayorType(data.getProductCategory() + " - " + data.getProductCategory());
										else
											data.setPayorType(data.getProductCategory() + " - " + data.getArCode());
										data.setArCode("");
									}									
										
									if(rsCategory.getString("EXCLUDE_FLAG") != null)
										excludeFlag = true;	
								}								
								else
								{
									data.setProductCategory("");
									data.setArCode("");								
								}								
									
								stmtCategory.close();															
							}
							else
							{
								data.setProductCategory("");
								data.setArCode("");							
							}
						}												
						stmtProduct.close();												
					}
					if(chargeFound)
					{
						data.setChargeID(rs.getString("ID"));
						data.setParArea(rs.getString("PAR_AREA"));
						data.setRecurringFlag(rs.getString("RECURRING_FLAG"));	
						data.setTotal(outNumberFormat.format(price * qty));				
						data.setQuantity(rs.getString("QUANTITY"));
						data.setPrice(rs.getString("PRICE"));
						data.setFrequency(rs.getString("FREQUENCY"));
						if(rs.getTimestamp("START_DATE") != null)
							data.setStartDate(dateFormat.format(rs.getTimestamp("START_DATE")));
						else
							data.setStartDate("");
						if(rs.getTimestamp("END_DATE") != null)	
							data.setEndDate(dateFormat.format(rs.getTimestamp("END_DATE")));
						else
							data.setEndDate("");
						data.setChargeDate(dateTimeFormat.format(rs.getTimestamp("CHARGE_DATE")));
						if(data.getPayorType() != null && data.getPayorType().equalsIgnoreCase(""))					
							data.setPayorType(getChargePayorType(reportCriteria.customer,reportCriteria.residentGUID,rs.getTimestamp("CHARGE_DATE")));
					
						if(reportCriteria.billExclusion.equalsIgnoreCase("N"))
						{
							if(!excludeFlag)
								chargeList.add(data);	
						}
						else if(reportCriteria.billExclusion.equalsIgnoreCase("B"))
						{
							if(excludeFlag)
								chargeList.add(data);						
						}
						else
						{
							chargeList.add(data);
						}
						
					}										
				}
			}
			stmt.close();
			
			logger.exiting(methodName);

			return chargeList;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getStandardCharges: "
					+ stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}						
	} 

	private List getRecurringCharges(ReportCriteria reportCriteria)throws ParScanResidentsEJBException
	{
		String methodName = "getRecurringCharges()";
		logger.entering(methodName);
		
		PreparedStatement stmt;
		PreparedStatement stmtMulti;
		PreparedStatement stmtService;
		PreparedStatement stmtProduct;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;		
		DateTime scheduleEndDate = null;
		DateTime scheduleCompareEndDate = null;
		DateTime scheduleStartDate = null;
		boolean excludeFlag = false;
		Connection conn = null;
		List recurringList = new ArrayList();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy h:mm:ss aa");
		String query = null;		

		try 
		{
			Timestamp sqlStartDate = new Timestamp(dateFormat.parse(dateFormat.format(reportCriteria.startDate)).getTime());
			DateTime eDate = new DateTime(reportCriteria.endDate.getTime());
			eDate = eDate.plusDays(1);			
			Timestamp sqlEndDate = new Timestamp(new java.util.Date(eDate.getMillis()).getTime());			
			Timestamp sqlPriceChangeDate;			
			conn = openConnection();
			
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE RESIDENT_GUID = ? AND CUSTOMER = ? AND START_DATE < ? AND RECURRING_FLAG = 'X' ORDER BY CHARGE_ID ASC");
			
			stmt.setString(1, reportCriteria.residentGUID);
			stmt.setString(2, reportCriteria.customer);
			stmt.setTimestamp(3,sqlEndDate);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) 
			{
				excludeFlag = false;
				DateTime searchStartDate = new DateTime(dateFormat.parse(dateFormat.format(reportCriteria.startDate)).getTime());
				DateTime searchEndDate = new DateTime(dateFormat.parse(dateFormat.format(reportCriteria.endDate)).getTime());
				scheduleStartDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("START_DATE"))).getTime());
				
				if(rs.getTimestamp("END_DATE") != null)
				{
					scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("END_DATE"))).getTime());	
					scheduleCompareEndDate = scheduleEndDate;
				}					
				else
				{
					scheduleEndDate = null;
					scheduleCompareEndDate = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())));					
				}
				
				if(scheduleCompareEndDate.isAfter(searchStartDate) || scheduleCompareEndDate.equals(searchStartDate))
				{
					ParScanResidentChargeBean data = new ParScanResidentChargeBean();
					
					if (rs.getString("SERVICE_FLAG") != null) 
					{							
							//Get service data
							query = reportCriteria.allCategories ? sqlProp.getString( "selectServiceCharge") : 
														sqlProp.getString( "selectServiceChargeForCategory").replaceFirst("#productCategoryCriteria#", reportCriteria.getCategorySqlCriteria());
							logger.debugT("RecurringCharges: service query =" + query);
							
							stmtService = conn.prepareStatement( query );
							stmtService.setString(1, rs.getString("CHARGE_ID"));
							stmtService.setString(2, reportCriteria.customer);
							ResultSet rsService = stmtService.executeQuery();
							
							if(rsService.next())
							{
								data.setServiceFlag("X");
								data.setCharge(rsService.getString("SERVICE"));
								data.setAltBarcode(rsService.getString("SERVICE"));
								data.setDescription(rsService.getString("DESCRIPTION"));
								data.setUom("");					
								data.setVendor("");
								data.setVendorItem("");
								data.setVendorUOM("");
								data.setCasePackaging("");
								data.setCost("");
								data.setParArea("");				
								
								if(rsService.getString("CATEGORY_ID") != null)
								{
									stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
									stmtCategory.setString(1, reportCriteria.customer);
									stmtCategory.setString(2, rsService.getString("CATEGORY_ID"));
									ResultSet rsCategory = stmtCategory.executeQuery();
									
									if(rsCategory.next())
									{								
										data.setProductCategory(rsCategory.getString("PRODUCT_CATEGORY"));								

										if(rsCategory.getString("AR_CODE") == null)
											data.setArCode("");
										else
											data.setArCode(rsCategory.getString("AR_CODE"));									

										if(rsCategory.getString("OVERRIDE_FLAG") != null)
										{
											if(data.getArCode().equalsIgnoreCase(""))
												data.setPayorType(data.getProductCategory() + " - " + data.getProductCategory());
											else
												data.setPayorType(data.getProductCategory() + " - " + data.getArCode());
											data.setArCode("");
										}
									
										if(rsCategory.getString("EXCLUDE_FLAG") != null)
											excludeFlag = true;	
									}								
									else
									{
										data.setProductCategory("");
										data.setArCode("");								
									}								
								
									stmtCategory.close();															
								}
								else
								{
									data.setProductCategory("");
									data.setArCode("");							
								}

								stmtService.close();								
									
							
								//charge data
								data.setChargeID(rs.getString("ID"));
								data.setRecurringFlag(rs.getString("RECURRING_FLAG"));				
								data.setQuantity(rs.getString("QUANTITY"));
								data.setPrice(rs.getString("PRICE"));
								data.setFrequency(rs.getString("FREQUENCY"));
								java.util.Date date = new java.util.Date(scheduleStartDate.getMillis());
								data.setStartDate(dateFormat.format(date));
								if(scheduleEndDate != null)
								{
									date = new java.util.Date(scheduleEndDate.getMillis());
									data.setEndDate(dateFormat.format(date));								
								}
								else
									data.setEndDate("");
								data.setPayorType(getChargePayorType(reportCriteria.customer, reportCriteria.residentGUID,rs.getTimestamp("CHARGE_DATE")));
								data.setChargeDate(dateTimeFormat.format(rs.getTimestamp("CHARGE_DATE")));
								
								setTotal(
									scheduleCompareEndDate,
									scheduleStartDate,
									searchStartDate,
									searchEndDate,
									data);						
															
								if(reportCriteria.billExclusion.equalsIgnoreCase("N"))
								{
									if(!excludeFlag)
									recurringList.add(data);	
								}
								else if(reportCriteria.billExclusion.equalsIgnoreCase("B"))
								{
									if(excludeFlag)
										recurringList.add(data);						
								}
								else
								{
									recurringList.add(data);
								}																	
							}	
						//}							
					}
					else
					{							
							//Get item data
							query = reportCriteria.allCategories ? sqlProp.getString( "selectProductCharge") : 
														sqlProp.getString( "selectProductChargeForCategory").replaceFirst("#productCategoryCriteria#", reportCriteria.getCategorySqlCriteria());
							
							logger.debugT("RecurringCharges: product charge query =" + query);
						
							stmtProduct = conn.prepareStatement( query );
							stmtProduct.setString(1, rs.getString("CHARGE_ID"));
							stmtProduct.setString(2, reportCriteria.customer);
							
							ResultSet rsProduct = stmtProduct.executeQuery();
							if(rsProduct.next())
							{
								data.setServiceFlag("");
								data.setCharge(rsProduct.getString("ITEM_ID"));
								data.setDescription(rsProduct.getString("DESCRIPTION"));
								data.setUom(rsProduct.getString("BILL_UOM"));
								data.setVendorItem(rsProduct.getString("VENDOR_ITEM"));
								data.setVendorUOM(rsProduct.getString("VENDOR_UOM"));
								data.setCasePackaging(rsProduct.getString("CASE_PACKAGING"));
								data.setCost(rsProduct.getString("CURRENT_COST"));
								if(rsProduct.getString("ALTERNATE_BARCODE") == null)
									data.setAltBarcode("");
								else
									data.setAltBarcode(rsProduct.getString("ALTERNATE_BARCODE"));							
								if(rsProduct.getString("VENDOR_ID") != null)
								{
									stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
									stmtVendor.setString(1, reportCriteria.customer);
									stmtVendor.setString(2, rsProduct.getString("VENDOR_ID"));
									ResultSet rsVendor = stmtVendor.executeQuery();
									if(rsVendor.next())
										data.setVendor(rsVendor.getString("VENDOR_NAME"));
									else							
										data.setVendor("");
									
									stmtVendor.close();	
								}
								else
								{
									data.setVendor("");	
								}
								if(rsProduct.getString("CATEGORY_ID") != null)
								{
									stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
									stmtCategory.setString(1, reportCriteria.customer);
									stmtCategory.setString(2, rsProduct.getString("CATEGORY_ID"));
									ResultSet rsCategory = stmtCategory.executeQuery();
									if(rsCategory.next())
									{
										data.setProductCategory(rsCategory.getString("PRODUCT_CATEGORY"));
										if(rsCategory.getString("AR_CODE") == null)
											data.setArCode("");
										else
											data.setArCode(rsCategory.getString("AR_CODE"));
											
										if(rsCategory.getString("EXCLUDE_FLAG") != null)
											excludeFlag = true;											
									}								
									else
									{
										data.setProductCategory("");
										data.setArCode("");								
									}	
									
									stmtCategory.close();															
								}
								else
								{
									data.setProductCategory("");
									data.setArCode("");							
								}
											
								stmtProduct.close();	
								
								//charge data
								data.setChargeID(rs.getString("ID"));
								data.setParArea(rs.getString("PAR_AREA"));
								data.setRecurringFlag(rs.getString("RECURRING_FLAG"));				
								data.setQuantity(rs.getString("QUANTITY"));
								data.setPrice(rs.getString("PRICE"));
								data.setFrequency(rs.getString("FREQUENCY"));
								java.util.Date date = new java.util.Date(scheduleStartDate.getMillis());
								data.setStartDate(dateFormat.format(date));
								if(scheduleEndDate != null)
								{
									date = new java.util.Date(scheduleEndDate.getMillis());
									data.setEndDate(dateFormat.format(date));								
								}
								else
									data.setEndDate("");
								data.setPayorType(getChargePayorType(reportCriteria.customer, reportCriteria.residentGUID,rs.getTimestamp("CHARGE_DATE")));
								data.setChargeDate(dateTimeFormat.format(rs.getTimestamp("CHARGE_DATE")));
								setTotal(
									scheduleCompareEndDate,
									scheduleStartDate,
									searchStartDate,
									searchEndDate,
									data);
								
								if(reportCriteria.billExclusion.equalsIgnoreCase("N"))
								{
									if(!excludeFlag)
									recurringList.add(data);	
								}
								else if(reportCriteria.billExclusion.equalsIgnoreCase("B"))
								{
									if(excludeFlag)
										recurringList.add(data);						
								}
								else
								{
									recurringList.add(data);
								}
							}																										
					}
				}
			}			

		}
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getRecurringCharges: "+ stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
		
		logger.exiting(methodName);
		
		return recurringList;
	}

	private void setTotal(
		DateTime scheduleCompareEndDate,
		DateTime scheduleStartDate,
		DateTime searchStartDate,
		DateTime searchEndDate,
		ParScanResidentChargeBean data) 
	{
		double quantity = Double.parseDouble(data.getQuantity());
		double totalQuantity = getRecurringQuantity(data.getFrequency(), quantity, 
								scheduleStartDate, searchStartDate, scheduleCompareEndDate, searchEndDate);
		
		if (totalQuantity == 0 && quantity != 0) 
		{
			totalQuantity = quantity;
		}		
		DecimalFormat outNumberFormat = new DecimalFormat("0");
		data.setRecurringQuantity(outNumberFormat.format(totalQuantity));
		ParScanResidentsSSBean.logger.debugT("description : " + data.getDescription() + " recurringQuantity : " + totalQuantity + " price : " + data.getPrice());
		//set total
		data.setTotal(getRecurringTotal(totalQuantity, Double.parseDouble(data.getPrice())));	
	}

	private double getRecurringQuantity(String frequency, double quantity,
	DateTime scheduleStartDate, DateTime searchStartDate, DateTime scheduleEndDate, DateTime searchEndDate)
	{	
		int daysBetween = 0;
		int weeksBetween = 0;
		int monthsBetween = 0;
		int yearsBetween = 0;
		double total  = 0;
		ParScanResidentsSSBean.logger.debugT("scheduleStartDate: " + scheduleStartDate.toString() + ", searchStartDate: " + searchStartDate + ", scheduleEnDate: " + scheduleEndDate + ", searchEndDate:" + searchEndDate);			
		ParScanResidentsSSBean.logger.debugT("frequency : " + frequency + " quantity : " + quantity);
		
		if(scheduleStartDate.isBefore(searchStartDate))
		{
			if(scheduleEndDate.isBefore(searchEndDate))
			{
				Days days = Days.daysBetween(searchStartDate, scheduleEndDate);	
				daysBetween = days.getDays();
				
				Weeks weeks = Weeks.weeksBetween(searchStartDate, scheduleEndDate);
				weeksBetween = weeks.getWeeks();				
								
				Months months = Months.monthsBetween(searchStartDate, scheduleEndDate);
				monthsBetween = months.getMonths();
								
				Years years = Years.yearsBetween(searchStartDate, scheduleEndDate);
				yearsBetween = years.getYears();								
			}
			else
			{
				Days days = Days.daysBetween(searchStartDate, searchEndDate);	
				daysBetween = days.getDays();

				Weeks weeks = Weeks.weeksBetween(searchStartDate, searchEndDate);
				weeksBetween = weeks.getWeeks();		
								
				Months months = Months.monthsBetween(searchStartDate, searchEndDate);
				monthsBetween = months.getMonths();
								
				Years years = Years.yearsBetween(searchStartDate, searchEndDate);
				yearsBetween = years.getYears();								
			}								
		}
		else
		{
			if(scheduleEndDate.isBefore(searchEndDate))
			{
				Days days = Days.daysBetween(scheduleStartDate, scheduleEndDate);	
				daysBetween = days.getDays();
				
				Weeks weeks = Weeks.weeksBetween(scheduleStartDate, scheduleEndDate);
				weeksBetween = weeks.getWeeks();
												
				Months months = Months.monthsBetween(scheduleStartDate, scheduleEndDate);
				monthsBetween = months.getMonths();
 								
				Years years = Years.yearsBetween(scheduleStartDate, scheduleEndDate);
				yearsBetween = years.getYears();								
			}
			else
			{
				Days days = Days.daysBetween(scheduleStartDate, searchEndDate);	
				daysBetween = days.getDays();	

				Weeks weeks = Weeks.weeksBetween(scheduleStartDate, searchEndDate);
				weeksBetween = weeks.getWeeks();
								
				Months months = Months.monthsBetween(searchStartDate, searchEndDate);
				monthsBetween = months.getMonths();
								
				Years years = Years.yearsBetween(searchStartDate, searchEndDate);
				yearsBetween = years.getYears();								
			}								
		}	
		
		frequency = frequency.toUpperCase();							
						
		if(frequency.equals("DAILY"))
		{
			total = quantity * (daysBetween+1);
		}
		else if(frequency.equals("WEEKLY"))
		{
			total = quantity * (weeksBetween+1);
		}
		else if(frequency.equals("MONTHLY"))
		{
			total = quantity * (monthsBetween+1);				
		}
		else if(frequency.equals("YEARLY"))
		{
			total = quantity * (yearsBetween+1);
		}		
		ParScanResidentsSSBean.logger.debugT("Recurring Quantity : " + total);
		return total;
	}

	private String getRecurringTotal(double quantity, double price)
	{			
		if (quantity == 0 || price == 0)
		{
			return "0.00";	
		}
		double total = price * quantity;
		DecimalFormat outNumberFormat = new DecimalFormat("0.00");
		return outNumberFormat.format(total);
	}

	private List getExpandedRecurringCharges(ReportCriteria reportCriteria) throws ParScanResidentsEJBException
	{
		PreparedStatement stmt;
		PreparedStatement stmtMulti;
		PreparedStatement stmtService;
		PreparedStatement stmtProduct;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;		
		DateTime scheduleEndDate = null;
		DateTime scheduleStartDate = null;
		boolean excludeFlag = false;
		Connection conn = null;
		List recurringList = new ArrayList();	
		DecimalFormat outNumberFormat = new DecimalFormat("0.00");
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy h:mm:ss aa");
		
		try 
		{
			Timestamp sqlStartDate = new Timestamp(dateFormat.parse(dateFormat.format(reportCriteria.startDate)).getTime());
			DateTime eDate = new DateTime(reportCriteria.endDate.getTime());
			eDate = eDate.plusDays(1);			
			Timestamp sqlEndDate = new Timestamp(new java.util.Date(eDate.getMillis()).getTime());			
			Timestamp sqlPriceChangeDate;			
			conn = openConnection();
			
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE RESIDENT_GUID = ? AND CUSTOMER = ? AND START_DATE < ? AND RECURRING_FLAG = 'X' ORDER BY CHARGE_ID ASC");
			
			stmt.setString(1, reportCriteria.residentGUID);
			stmt.setString(2, reportCriteria.customer);
			stmt.setTimestamp(3,sqlEndDate);
			//stmt.setTimestamp(4,sqlStartDate);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) 
			{
				excludeFlag = false;
				DateTime searchStartDate = new DateTime(dateFormat.parse(dateFormat.format(reportCriteria.startDate)).getTime());
				DateTime searchEndDate = new DateTime(dateFormat.parse(dateFormat.format(reportCriteria.endDate)).getTime());
				scheduleStartDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("START_DATE"))).getTime());
				
				if(rs.getTimestamp("END_DATE") != null)
					scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("END_DATE"))).getTime());
				else
					scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())));
				
				if(scheduleEndDate.isAfter(searchStartDate) || scheduleEndDate.equals(searchStartDate))
				{
					ParScanResidentChargeBean data = new ParScanResidentChargeBean();
					
					if (rs.getString("SERVICE_FLAG") != null) 
					{							
						//Get service data
						stmtService = conn.prepareStatement("SELECT * FROM PARSCAN_SERVICES WHERE ID = ? AND CUSTOMER = ?");
						stmtService.setString(1, rs.getString("CHARGE_ID"));
						stmtService.setString(2, reportCriteria.customer);
						ResultSet rsService = stmtService.executeQuery();
						rsService.next();
						data.setServiceFlag("X");
						data.setCharge(rsService.getString("SERVICE"));
						data.setAltBarcode(rsService.getString("SERVICE"));
						data.setDescription(rsService.getString("DESCRIPTION"));
						data.setUom("");					
						data.setVendor("");
						data.setVendorItem("");
						data.setVendorUOM("");
						data.setCasePackaging("");
						data.setCost("");
						data.setProductCategory("");	
						data.setParArea("");	
						data.setPayorType("");		
						
						if(rsService.getString("CATEGORY_ID") != null)
						{
							stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
							stmtCategory.setString(1, reportCriteria.customer);
							stmtCategory.setString(2, rsService.getString("CATEGORY_ID"));
							ResultSet rsCategory = stmtCategory.executeQuery();
							if(rsCategory.next())
							{
								data.setProductCategory(rsCategory.getString("PRODUCT_CATEGORY"));
								if(rsCategory.getString("AR_CODE") == null)
									data.setArCode("");
								else
									data.setArCode(rsCategory.getString("AR_CODE"));
									
								if(rsCategory.getString("OVERRIDE_FLAG") != null)
								{
									if(data.getArCode().equalsIgnoreCase(""))
										data.setPayorType(data.getProductCategory() + " - " + data.getProductCategory());
									else
										data.setPayorType(data.getProductCategory() + " - " + data.getArCode());
									data.setArCode("");
								}										
									
								if(rsCategory.getString("EXCLUDE_FLAG") != null)
									excludeFlag = true;	
							}								
							else
							{
								data.setProductCategory("");
								data.setArCode("");								
							}								
								
							stmtCategory.close();															
						}
						else
						{
							data.setProductCategory("");
							data.setArCode("");							
						}													
						stmtService.close();	
						
						//charge data
						data.setChargeID(rs.getString("ID"));
						data.setRecurringFlag(rs.getString("RECURRING_FLAG"));				
						data.setQuantity(rs.getString("QUANTITY"));
						data.setPrice(rs.getString("PRICE"));
						data.setFrequency(rs.getString("FREQUENCY"));
						java.util.Date date = new java.util.Date(scheduleStartDate.getMillis());
						data.setStartDate(dateFormat.format(date));
						date = new java.util.Date(scheduleEndDate.getMillis());
						data.setEndDate(dateFormat.format(date));
						if(data.getPayorType().equalsIgnoreCase(""))
							data.setPayorType(getChargePayorType(reportCriteria.customer, reportCriteria.residentGUID,rs.getTimestamp("CHARGE_DATE")));
						data.setChargeDate(dateTimeFormat.format(rs.getTimestamp("CHARGE_DATE")));

						//set total						
						data.setTotal(outNumberFormat.format(Double.parseDouble(data.getPrice()) * Double.parseDouble(data.getQuantity())));		
						
						if(reportCriteria.billExclusion.equalsIgnoreCase("N"))
						{
							if(!excludeFlag)
								recurringList.addAll(getBreakOutList(data, searchStartDate, searchEndDate, scheduleStartDate, scheduleEndDate));	
						}
						else if(reportCriteria.billExclusion.equalsIgnoreCase("B"))
						{
							if(excludeFlag)
								recurringList.addAll(getBreakOutList(data, searchStartDate, searchEndDate, scheduleStartDate, scheduleEndDate));						
						}
						else
						{
							recurringList.addAll(getBreakOutList(data, searchStartDate, searchEndDate, scheduleStartDate, scheduleEndDate));
						}																				
					}
					else
					{							
						//Get item data
						stmtProduct = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE ID = ? AND CUSTOMER = ?");
						stmtProduct.setString(1, rs.getString("CHARGE_ID"));
						stmtProduct.setString(2, reportCriteria.customer);
						ResultSet rsProduct = stmtProduct.executeQuery();
						rsProduct.next();
						data.setServiceFlag("");
						data.setPayorType("");
						data.setCharge(rsProduct.getString("ITEM_ID"));
						data.setDescription(rsProduct.getString("DESCRIPTION"));
						data.setUom(rsProduct.getString("BILL_UOM"));
						data.setVendorUOM(rsProduct.getString("VENDOR_UOM"));
						data.setCasePackaging(rsProduct.getString("CASE_PACKAGING"));
						data.setCost(rsProduct.getString("CURRENT_COST"));
						if(rsProduct.getString("ALTERNATE_BARCODE") == null)
							data.setAltBarcode("");
						else
							data.setAltBarcode(rsProduct.getString("ALTERNATE_BARCODE"));							
						if(rsProduct.getString("VENDOR_ID") != null)
						{
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
							stmtVendor.setString(1, reportCriteria.customer);
							stmtVendor.setString(2, rsProduct.getString("VENDOR_ID"));
							ResultSet rsVendor = stmtVendor.executeQuery();
							if(rsVendor.next())
								data.setVendor(rsVendor.getString("VENDOR_NAME"));
							else							
								data.setVendor("");
							
							stmtVendor.close();	
						}
						else
						{
							data.setVendor("");	
						}
						if(rsProduct.getString("CATEGORY_ID") != null)
						{
							stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
							stmtCategory.setString(1, reportCriteria.customer);
							stmtCategory.setString(2, rsProduct.getString("CATEGORY_ID"));
							ResultSet rsCategory = stmtCategory.executeQuery();
							if(rsCategory.next())
							{
								data.setProductCategory(rsCategory.getString("PRODUCT_CATEGORY"));
								if(rsCategory.getString("AR_CODE") == null)
									data.setArCode("");
								else
									data.setArCode(rsCategory.getString("AR_CODE"));
									
								if(rsCategory.getString("OVERRIDE_FLAG") != null)
								{
									if(data.getArCode().equalsIgnoreCase(""))
										data.setPayorType(data.getProductCategory() + " - " + data.getProductCategory());
									else
										data.setPayorType(data.getProductCategory() + " - " + data.getArCode());
									data.setArCode("");
								}										
									
								if(rsCategory.getString("EXCLUDE_FLAG") != null)
									excludeFlag = true;											
							}								
							else
							{
								data.setProductCategory("");
								data.setArCode("");								
							}	
							
							stmtCategory.close();															
						}
						else
						{
							data.setProductCategory("");
							data.setArCode("");							
						}
										
						stmtProduct.close();	
						
						//charge data
						data.setChargeID(rs.getString("ID"));
						data.setParArea(rs.getString("PAR_AREA"));
						data.setRecurringFlag(rs.getString("RECURRING_FLAG"));				
						data.setQuantity(rs.getString("QUANTITY"));
						data.setPrice(rs.getString("PRICE"));
						data.setFrequency(rs.getString("FREQUENCY"));
						java.util.Date date = new java.util.Date(scheduleStartDate.getMillis());
						data.setStartDate(dateFormat.format(date));
						date = new java.util.Date(scheduleEndDate.getMillis());
						data.setEndDate(dateFormat.format(date));
						if(data.getPayorType().equalsIgnoreCase(""))
							data.setPayorType(getChargePayorType(reportCriteria.customer, reportCriteria.residentGUID,rs.getTimestamp("CHARGE_DATE")));
						data.setChargeDate(dateTimeFormat.format(rs.getTimestamp("CHARGE_DATE")));

						//set total
						data.setTotal(outNumberFormat.format(Double.parseDouble(data.getPrice()) * Double.parseDouble(data.getQuantity())));
						
						if(reportCriteria.billExclusion.equalsIgnoreCase("N"))
						{
							if(!excludeFlag)
								recurringList.addAll(getBreakOutList(data, searchStartDate, searchEndDate, scheduleStartDate, scheduleEndDate));	
						}
						else if(reportCriteria.billExclusion.equalsIgnoreCase("B"))
						{
							if(excludeFlag)
								recurringList.addAll(getBreakOutList(data, searchStartDate, searchEndDate, scheduleStartDate, scheduleEndDate));						
						}
						else
						{
							recurringList.addAll(getBreakOutList(data, searchStartDate, searchEndDate, scheduleStartDate, scheduleEndDate));
						}														
					}
				}
			}			

		}
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getExpandedRecurringCharges: "+ stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
		return recurringList;
	}

	private List getBreakOutList(ParScanResidentChargeBean data, DateTime searchStartDate, DateTime searchEndDate, DateTime scheduleStartDate, DateTime scheduleEndDate) throws ParScanResidentsEJBException
	{
		List breakOutList = new ArrayList();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try
		{
			int daysBetween = 0;
			int weeksBetween = 0;
			int monthsBetween = 0;
			int yearsBetween = 0;
			double total  = 0;
			ParScanResidentsSSBean.logger.debugT("scheduleStartDate: " + scheduleStartDate.toString() + ", searchStartDate: " + searchStartDate + ", scheduleEnDate: " + scheduleEndDate + ", searchEndDate:" + searchEndDate);			
			
			if(scheduleStartDate.isBefore(searchStartDate))
			{
				if(scheduleEndDate.isBefore(searchEndDate))
				{
					Days days = Days.daysBetween(searchStartDate, scheduleEndDate);	
					daysBetween = days.getDays();
				
					Weeks weeks = Weeks.weeksBetween(searchStartDate, scheduleEndDate);
					weeksBetween = weeks.getWeeks();				
								
					Months months = Months.monthsBetween(searchStartDate, scheduleEndDate);
					monthsBetween = months.getMonths();
								
					Years years = Years.yearsBetween(searchStartDate, scheduleEndDate);
					yearsBetween = years.getYears();								
				}
				else
				{
					Days days = Days.daysBetween(searchStartDate, searchEndDate);	
					daysBetween = days.getDays();

					Weeks weeks = Weeks.weeksBetween(searchStartDate, searchEndDate);
					weeksBetween = weeks.getWeeks();		
								
					Months months = Months.monthsBetween(searchStartDate, searchEndDate);
					monthsBetween = months.getMonths();
								
					Years years = Years.yearsBetween(searchStartDate, searchEndDate);
					yearsBetween = years.getYears();								
				}								
			}
			else
			{
				if(scheduleEndDate.isBefore(searchEndDate))
				{
					Days days = Days.daysBetween(scheduleStartDate, scheduleEndDate);	
					daysBetween = days.getDays();
				
					Weeks weeks = Weeks.weeksBetween(scheduleStartDate, scheduleEndDate);
					weeksBetween = weeks.getWeeks();
												
					Months months = Months.monthsBetween(scheduleStartDate, scheduleEndDate);
					monthsBetween = months.getMonths();
 								
					Years years = Years.yearsBetween(scheduleStartDate, scheduleEndDate);
					yearsBetween = years.getYears();								
				}
				else
				{
					Days days = Days.daysBetween(scheduleStartDate, searchEndDate);	
					daysBetween = days.getDays();	

					Weeks weeks = Weeks.weeksBetween(scheduleStartDate, searchEndDate);
					weeksBetween = weeks.getWeeks();
								
					Months months = Months.monthsBetween(searchStartDate, searchEndDate);
					monthsBetween = months.getMonths();
								
					Years years = Years.yearsBetween(searchStartDate, searchEndDate);
					yearsBetween = years.getYears();								
				}								
			}	
							
						
			if(data.getFrequency().equalsIgnoreCase("Daily"))
			{
				total = daysBetween+1;
			}
			else if(data.getFrequency().equalsIgnoreCase("Weekly"))
			{
				total = weeksBetween+1;
			}
			else if(data.getFrequency().equalsIgnoreCase("Monthly"))
			{
				total = monthsBetween+1;				
			}
			else if(data.getFrequency().equalsIgnoreCase("Yearly"))
			{
				total = yearsBetween+1;
			}						
		
			DateTime beginChargeDate = null; 
			
			if(scheduleStartDate.isBefore(searchStartDate))
			{
				//Return searchDate
				//java.util.Date date = new java.util.Date(searchDate.getMillis());
				//recurringDate = sdf.format(date);
				beginChargeDate = searchStartDate;
			}
			else
			{
				//Return startDate
				beginChargeDate = scheduleStartDate;
			}
						
			for(int i = 0; i < total; i++)
			{
				ParScanResidentChargeBean breakOut = new ParScanResidentChargeBean();
				
				breakOut.setChargeID(data.getChargeID());
				breakOut.setParArea(data.getParArea());
				breakOut.setRecurringFlag(data.getRecurringFlag());				
				breakOut.setQuantity(data.getQuantity());
				breakOut.setPrice(data.getPrice());
				breakOut.setFrequency(data.getFrequency());
				breakOut.setStartDate(data.getStartDate());
				breakOut.setEndDate(data.getEndDate());
				breakOut.setPayorType(data.getPayorType());
				breakOut.setProductCategory(data.getProductCategory());
				breakOut.setArCode(data.getArCode());			
				breakOut.setVendor(data.getVendor());
				breakOut.setServiceFlag(data.getServiceFlag());
				breakOut.setCharge(data.getCharge());
				breakOut.setDescription(data.getDescription());
				breakOut.setUom(data.getUom());
				breakOut.setVendorUOM(data.getVendorUOM());
				breakOut.setCasePackaging(data.getCasePackaging());
				breakOut.setCost(data.getCost());
				breakOut.setTotal(data.getTotal());
				breakOut.setAltBarcode("");
				
				if(i > 0)
				{
					if(data.getFrequency().equalsIgnoreCase("Daily"))
					{
						beginChargeDate = beginChargeDate.plusDays(1);
					}
					else if(data.getFrequency().equalsIgnoreCase("Weekly"))
					{
						beginChargeDate = beginChargeDate.plusWeeks(1);
					}
					else if(data.getFrequency().equalsIgnoreCase("Monthly"))
					{
						beginChargeDate = beginChargeDate.plusMonths(1);
					}
					else if(data.getFrequency().equalsIgnoreCase("Yearly"))
					{
						beginChargeDate = beginChargeDate.plusYears(1);
					}					
				}
				
				//ParScanResidentsSSBean.logger.debugT(dateFormat.format(new java.util.Date(beginChargeDate.getMillis())));
				breakOut.setChargeDate(dateFormat.format(new java.util.Date(beginChargeDate.getMillis())));
				
				breakOutList.add(breakOut);			
			}
						
		}
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getBreakOutList: "+ stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		
		return breakOutList;
	}

	private String getChargePayorType(
		String customer,
		String residentGUID,
		Timestamp chargeDate)
	throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		PreparedStatement stmtPayor;
		Connection conn = null;
		String payer = "";
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try 
		{
			DateTime date = new DateTime(dateFormat.parse(dateFormat.format(chargeDate)).getTime());
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PAYORINFO WHERE RESIDENT_GUID = ? AND CUSTOMER = ? ORDER BY EFFECTIVE_DATE");
			stmt.setString(1, residentGUID);
			stmt.setString(2, customer);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) 
			{
				if(rs.getTimestamp("EFFECTIVE_DATE") != null)
				{
					DateTime effDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("EFFECTIVE_DATE"))).getTime());
					DateTime endDate = null;
					if(rs.getTimestamp("END_DATE") != null)
						endDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("END_DATE"))).getTime());
					else
						endDate = date;

					if ((date.equals(effDate) || date.equals(endDate)) || (date.isAfter(effDate) && date.isBefore(endDate))) 
					{
						stmtPayor = conn.prepareStatement("SELECT * FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
						stmtPayor.setString(1, customer);
						stmtPayor.setString(2, rs.getString("PAYOR_CODE"));

						ResultSet rsPayor = stmtPayor.executeQuery();
						rsPayor.next();

						payer = rsPayor.getString("PAYOR_CODE") + " - " + rsPayor.getString("DESCRIPTION");
						break;
					}					
				}
			}
			stmt.close();

			return payer;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getChargePayorType: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getServices(String customer) throws ParScanResidentsEJBException 
	{
		List serviceList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;

		try 
		{
			conn = openConnection();

			stmt =
				conn.prepareStatement(
					"SELECT * FROM PARSCAN_SERVICES WHERE CUSTOMER = ?");
			stmt.setString(1, customer);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) 
			{	
				String item = "";
				String description = "";							
				ParScanServiceBean data = new ParScanServiceBean();

				if(!rs.getString("ITEM_GUID").equalsIgnoreCase("NA"))
				{
					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmtItem.setString(1, customer);
					stmtItem.setString(2, rs.getString("ITEM_GUID"));
				
					ResultSet rsItem = stmtItem.executeQuery();
					if(rsItem.next())
					{				
						item = rsItem.getString("ITEM_ID");
						description = rsItem.getString("DESCRIPTION");
					}
				}

				data.setServiceGUID(rs.getString("ID"));
				data.setServiceName(rs.getString("SERVICE"));
				data.setDescription(rs.getString("DESCRIPTION"));
				data.setRelatedItem(item);
				data.setItemDescription(description);
				data.setFrequency(rs.getString("FREQUENCY"));
				data.setPrice(rs.getString("PRICE"));
				data.setServiceBarcode(rs.getString("SERVICE_BARCODE"));				

				serviceList.add(data);
			}
			stmt.close();

			return serviceList;
		}
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getServices: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public String checkCosts(String customer)throws ParScanResidentsEJBException 
	{
		String response = "N";
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND SHOW_COST_FLAG = 'Y'");
			stmt.setString(1, customer);

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") > 0) 
			{
				response = "Y";
			}
			stmt.close();

			return response;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during checkCosts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{		
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void acknowledgeCosts(String customer)throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			conn = openConnection();

			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET SHOW_COST_FLAG = 'N' WHERE CUSTOMER = ? AND SHOW_COST_FLAG = 'Y'");
			stmt.setString(1, customer);
			stmt.executeUpdate();
			stmt.close();

		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during acknowledgeCosts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{		
				e.printStackTrace();
			}
		}
	}	

	/**
	 * Business Method.
	 */
	public List getEmployeeList(String customerNumber, String empIdsCriteria) throws ParScanResidentsEJBException
	{
		List employeeeList = new ArrayList();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		String sqlQuery = "SELECT * FROM PARSCAN_EMPLOYEE WHERE CUSTOMER = ?";
		String sqlWhereClause = " AND EMPLOYEE_ID IN (#empIds#)";
		
		if( ! StringUtils.isEmpty( empIdsCriteria ) )
		{
			sqlWhereClause = sqlWhereClause.replaceAll( "#empIds#", empIdsCriteria );
			sqlQuery += sqlWhereClause;
		}
		
		PreparedStatement stmt = null;
		Connection conn = null;
		ResultSet rs = null;

		try 
		{
			conn = openConnection();

			stmt = conn.prepareStatement( sqlQuery );
			stmt.setString(1, customerNumber);

			rs = stmt.executeQuery();

			while (rs.next()) 
			{
				ParScanEmployeeBean data = new ParScanEmployeeBean();

				data.setId(rs.getString("EMPLOYEE_ID"));
				data.setFirstName(rs.getString("FIRST_NAME"));
				data.setLastName(rs.getString("LAST_NAME"));
				
				if(rs.getTimestamp("HIRE_DATE") != null)
				{
					data.setHireDate(dateFormat.format(rs.getTimestamp("HIRE_DATE")));				
				}
				else
				{
					data.setHireDate("");
				}
									
				if(rs.getTimestamp("TERMINATE_DATE") != null)
				{
					data.setEndDate(dateFormat.format(rs.getTimestamp("TERMINATE_DATE")));
				}
				else
				{
					data.setEndDate("");
				}
									
				employeeeList.add(data);
			}
			
			return employeeeList;
			
		} 
		catch( Exception ex ) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			logger.errorT( "Error during getEmployeeList: " + stringWriter.toString());
			
			throw new ParScanResidentsEJBException( ex );
		} 
		finally 
		{
			closeDbResources( conn, stmt, rs );
		}
	}

	/**
	 * Business Method.
	 */
	public void addEmployee(String customer, ParScanEmployeeBean newEmployee)
	throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		Connection conn = null;
		Timestamp sqlHireDate = null;
		Timestamp sqlEndDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try 
		{
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();

			stmt =
				conn.prepareStatement(
					"SELECT COUNT(*) AS CNT FROM PARSCAN_EMPLOYEE WHERE CUSTOMER = ? AND EMPLOYEE_ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, newEmployee.getId());

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") <= 0) 
			{
				stmt.close();
				
				if(!newEmployee.getHireDate().equalsIgnoreCase(""))
					sqlHireDate = new Timestamp(dateFormat.parse(newEmployee.getHireDate()).getTime());
				if(!newEmployee.getEndDate().equalsIgnoreCase(""))
					sqlEndDate = new Timestamp(dateFormat.parse(newEmployee.getEndDate()).getTime());
				
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_EMPLOYEE (ID, CUSTOMER, EMPLOYEE_ID, FIRST_NAME, LAST_NAME, HIRE_DATE, TERMINATE_DATE, PARSCAN_USER) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();

				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, newEmployee.getId());
				stmt.setString(4, newEmployee.getFirstName());
				stmt.setString(5, newEmployee.getLastName());
				stmt.setTimestamp(6, sqlHireDate);
				stmt.setTimestamp(7, sqlEndDate);
				stmt.setString(8, user);

				stmt.executeUpdate();
				stmt.close();
			}
			else
			{
				updateEmployee(customer, newEmployee.getId(), newEmployee);
			}
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addEmployee: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{		
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void updateEmployee(
		String customer,
		String oldID,
		ParScanEmployeeBean employee)
	throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		Connection conn = null;
		Timestamp sqlHireDate = null;
		Timestamp sqlEndDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try 
		{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			if(!employee.getHireDate().equalsIgnoreCase(""))
				sqlHireDate = new Timestamp(dateFormat.parse(employee.getHireDate()).getTime());
			if(!employee.getEndDate().equalsIgnoreCase(""))
				sqlEndDate = new Timestamp(dateFormat.parse(employee.getEndDate()).getTime());
							
			conn = openConnection();

			stmt = conn.prepareStatement("UPDATE PARSCAN_EMPLOYEE SET EMPLOYEE_ID = ?, FIRST_NAME = ?, LAST_NAME =?, HIRE_DATE = ?, TERMINATE_DATE = ?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND EMPLOYEE_ID = ?");

			stmt.setString(1, employee.getId());
			stmt.setString(2, employee.getFirstName());
			stmt.setString(3, employee.getLastName());
			stmt.setTimestamp(4, sqlHireDate);
			stmt.setTimestamp(5, sqlEndDate);
			stmt.setString(6, user);
			stmt.setString(7, customer);
			stmt.setString(8, oldID);

			stmt.executeUpdate();
			stmt.close();
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateEmployee: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{		
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void removeEmployee(String customer, List employeeData)
	throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();

			for (Iterator itor = employeeData.iterator(); itor.hasNext();) 
			{
				ParScanEmployeeBean data = (ParScanEmployeeBean) itor.next();
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_EMPLOYEE WHERE CUSTOMER = ? AND EMPLOYEE_ID = ?");

				stmt.setString(1, customer);
				stmt.setString(2, data.getId());

				stmt.executeUpdate();
				stmt.close();		
				removeScanLog(customer, data.getId());	
			}
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeEmployee: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{		
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getEmployeeScanLog(String customer, String id, Date startDate,Date endDate) throws ParScanResidentsEJBException 
	{
		List scanList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtCharge;
		PreparedStatement stmtDevice;
		ResultSet rsDevice;
		ResultSet rsCharge;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try 
		{
			Timestamp sqlStartDate = new Timestamp(startDate.getTime());						
			conn = openConnection();
			DateTime eDate = new DateTime(endDate.getTime());
			eDate = eDate.plusDays(1);			
			Timestamp sqlEndDate = new Timestamp(new java.util.Date(eDate.getMillis()).getTime());

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_SCAN_LOG WHERE CUSTOMER = ? AND EMPLOYEE_ID = ? AND SCAN_DATE BETWEEN ? AND ? ORDER BY RESIDENT_ID");
			stmt.setString(1, customer);
			stmt.setString(2, id);
			stmt.setTimestamp(3,sqlStartDate);
			stmt.setTimestamp(4,sqlEndDate);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) 
			{
				ParScanEmployeeScanBean data = new ParScanEmployeeScanBean();	
				if (rs.getString("SERVICE_FLAG") == null) {
				//GET ITEM DATA
					stmtCharge = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmtCharge.setString(1, customer);
					stmtCharge.setString(2, rs.getString("CHARGE_ID"));
					rsCharge = stmtCharge.executeQuery();
					if(rsCharge.next())
					{
						data.setCharge(rsCharge.getString("ITEM_ID"));
						data.setChargeDescription(rsCharge.getString("DESCRIPTION"));						
					}
					else
					{
						data.setCharge("");
						data.setChargeDescription("ITEM REMOVED");
					}

					data.setChargeQuantity(rs.getString("CHARGE_QUANTITY"));
					stmtCharge.close();
				}
				else
				{
				//	GET SERVICE DATA
					stmtCharge = conn.prepareStatement("SELECT * FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND ID = ?");
					stmtCharge.setString(1, customer);
					stmtCharge.setString(2, rs.getString("CHARGE_ID"));
					rsCharge = stmtCharge.executeQuery();			
					if(rsCharge.next())
					{
						data.setCharge(rsCharge.getString("SERVICE"));
						data.setChargeDescription(rsCharge.getString("DESCRIPTION"));						
					}
					else
					{
						data.setCharge("");
						data.setChargeDescription("SERVICE REMOVED");						
					}

					data.setChargeQuantity(rs.getString("CHARGE_QUANTITY"));
					stmtCharge.close();							
				}
										
				if(!rs.getString("RESIDENT_ID").trim().equalsIgnoreCase(""))
				{
					data.setResidentID(rs.getString("RESIDENT_ID"));
					data.setResidentFirstName(rs.getString("FIRST_NAME"));
					data.setResidentLastName(rs.getString("LAST_NAME"));				
				}
				else
				{
					ParScanResidentBean resData = new ParScanResidentBean();
					resData = getResidentName(customer, rs.getString("RESIDENT_GUID"));
					data.setResidentID(resData.getId());
					data.setResidentFirstName(resData.getFirstName());
					data.setResidentLastName(resData.getLastName());					
				}

				
				stmtDevice = conn.prepareStatement("SELECT * FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ?");
				stmtDevice.setString(1, rs.getString("DEVICE_KEY"));
				rsDevice = stmtDevice.executeQuery();
				if(rsDevice.next())
				{
					data.setScannerUsed(rsDevice.getString("DEVICE_NAME"));					
				}
				else
				{
					data.setScannerUsed("SCANNER REMOVED");
				}
	
				data.setScanID(rs.getString("ID"));
				data.setScanDate(dateFormat.format(rs.getTimestamp("SCAN_DATE")));
				

				scanList.add(data);
				stmtDevice.close();
			}
			stmt.close();

			return scanList;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getEmployeeScanLog: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{		
				e.printStackTrace();
			}
		}
	}

	private ParScanResidentBean getResidentName(String customer, String guid) throws ParScanResidentsEJBException 
	{
		ParScanResidentBean data = new ParScanResidentBean();
		IConnection connection = null;
	
		try 
		{
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "ZV_PATIENT_SEARCH");

			IFunctionsMetaData functionsMetaData = connection.getFunctionsMetaData();
			IFunction function = functionsMetaData.getFunction("ZV_PATIENT_SEARCH");

			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams = recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory = interaction.retrieveStructureFactory();

			importParams.put("I_SOLDTO", customer);

			MappedRecord exportParams = (MappedRecord) interaction.execute(	interactionSpec, importParams );
			IRecordSet residents = (IRecordSet) exportParams.get("T_PATIENT_DATA");

			while (residents.next()) 
			{
				if( residents.getString("GUID").equalsIgnoreCase( guid ))
				{
					data.setId(residents.getString("PATIENT_ID"));
					data.setFirstName(residents.getString("PFNAME"));
					data.setLastName(residents.getString("PLNAME"));
					break;	
				}
			}

		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT( "Error during getResidentName: " + stringWriter.toString());
			
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (connection != null)
					connection.close();
			} 
			catch (ResourceException ex) 
			{
				logger.errorT(" Failed to close CRM connection, " + ex.toString() );
			}
		}

		return data;
	}
	/**
	 * Business Method.
	 */
	public void removeScanLog(String customer, String id) throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("DELETE FROM PARSCAN_SCAN_LOG WHERE CUSTOMER = ? AND EMPLOYEE_ID = ?");

			stmt.setString(1, customer);
			stmt.setString(2, id);

			stmt.executeUpdate();
			stmt.close();
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeScanLog: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{	
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getVendors(String customer) throws ParScanResidentsEJBException 
	{
		List vendorList = new ArrayList();
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?");
			stmt.setString(1, customer);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) 
			{
				ParScanVendorBean data = new ParScanVendorBean();

				data.setVendorGUID(rs.getString("ID"));
				data.setVendorID(rs.getString("VENDOR_ID"));
				data.setVendorName(rs.getString("VENDOR_NAME"));
				data.setNotes(rs.getString("NOTES"));
				data.setAddress(rs.getString("ADDRESS"));
				data.setCity(rs.getString("CITY"));
				data.setState(rs.getString("VENDOR_STATE"));
				data.setZipcode(rs.getString("ZIP_CODE"));
				data.setPhone(rs.getString("PHONE"));
				data.setFax(rs.getString("FAX"));

				vendorList.add(data);
			}
			stmt.close();

			return vendorList;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getVendors: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getPayorInfromation(String customer) throws ParScanResidentsEJBException 
	{
		List payerList = new ArrayList();
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			conn = openConnection();
			HashMap distinctPC = new HashMap();
			
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ?");
			stmt.setString(1, customer);
			ResultSet rs = stmt.executeQuery();			

			while (rs.next())
			{			
				ParScanPayorCodeBean data = (ParScanPayorCodeBean) distinctPC.get(rs.getString("PAYOR_CODE"));

				if (data == null) 
				{
					ParScanPayorCodeBean pData = new ParScanPayorCodeBean();
					pData.setPayorCode(rs.getString("PAYOR_CODE"));
					pData.setPayorDescription(rs.getString("DESCRIPTION"));
					pData.setNotes(rs.getString("NOTES"));

					distinctPC.put(pData.getPayorCode(), pData);
					payerList.add(pData);
				}
			}
			stmt.close();

			return payerList;
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getPayorInformation: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{			
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void addVendor(String customer, ParScanVendorBean vendor) throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			int count = 0;
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			if(!vendor.getVendorID().equalsIgnoreCase(""))
			{
				stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, vendor.getVendorID());

				ResultSet rs = stmt.executeQuery();
				rs.next();
				count = rs.getInt("CNT");
				stmt.close();				
			}

			if (count <= 0) 
			{
				stmt = conn.prepareStatement(
						"INSERT INTO PARSCAN_VENDOR (ID, CUSTOMER, VENDOR_ID, VENDOR_NAME, NOTES, PARSCAN_USER, ADDRESS, CITY, VENDOR_STATE, ZIP_CODE, PHONE, FAX) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();

				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, emptyString(vendor.getVendorID()));
				stmt.setString(4, vendor.getVendorName());
				stmt.setString(5, emptyString(vendor.getNotes()));
				stmt.setString(6, user);
				stmt.setString(7, emptyString(vendor.getAddress()));
				stmt.setString(8, emptyString(vendor.getCity()));
				stmt.setString(9, emptyString(vendor.getState()));
				stmt.setString(10, emptyString(vendor.getZipcode()));
				stmt.setString(11, emptyString(vendor.getPhone()));
				stmt.setString(12, emptyString(vendor.getFax()));

				stmt.executeUpdate();
				stmt.close();
			}
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addVendor: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{		
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void addPayor(String customer, ParScanPayorCodeBean payerInfo) throws ParScanResidentsEJBException 
	{
		PreparedStatement stmt;
		Connection conn = null;

		try 
		{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
			stmt.setString(1, customer);
			stmt.setString(2, payerInfo.getPayorCode());

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") <= 0) 
			{
				stmt.close();
				
				if(payerInfo.getPayorDescription().equalsIgnoreCase(""))
					payerInfo.setPayorDescription(payerInfo.getPayorCode());
				
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_PAYORCODES (ID, CUSTOMER, PAYOR_CODE, DESCRIPTION, NOTES, PARSCAN_USER, CATEGORY_ID) VALUES (?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();

				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, payerInfo.getPayorCode());
				stmt.setString(4, payerInfo.getPayorDescription());
				stmt.setString(5, emptyString(payerInfo.getNotes()));
				stmt.setString(6, user);
				stmt.setString(7, null);

				stmt.executeUpdate();
				stmt.close();
			}
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addPayor: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{			
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void removeVendorProducts(String customer, List itemList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanItemBean data = (ParScanItemBean) itor.next();
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_KIT WHERE CUSTOMER = ? AND ITEM_GUID = ?");					
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();							
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_SCAN_LOG WHERE CUSTOMER = ? AND CHARGE_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();			
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND ITEM_GUID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();
			
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND ITEM_GUID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();

				stmt = conn.prepareStatement("DELETE FROM PARSCAN_STOCK_LOG WHERE CUSTOMER = ? AND ITEM_GUID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();
			
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND CHARGE_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();
			
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_GUID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();	
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void removeKitProducts(String customer, String itemID, List itemList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		
		try {
			conn = openConnection();

			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanItemBean data = (ParScanItemBean) itor.next();
								
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_KIT WHERE CUSTOMER = ? AND KIT_ID = ? AND ITEM_GUID = ?");					
				stmt.setString(1, customer);
				stmt.setString(2, itemID);
				stmt.setString(3, data.getItemGUID());
				stmt.executeUpdate();
				stmt.close();											
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeKitProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void removeProducts(String customer, List itemList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		List guids = new ArrayList();
		
		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanItemBean data = (ParScanItemBean) itor.next();
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemID());
				ResultSet rs = stmt.executeQuery();
				while(rs.next()){
					guids.add(rs.getString("ID"));
				}
				stmt.close();				
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemID());
				stmt.executeUpdate();
				stmt.close();
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_KIT WHERE CUSTOMER = ? AND KIT_ID = ?");					
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemID());
				stmt.executeUpdate();
				stmt.close();							
				
				for (Iterator guidItor = guids.iterator(); guidItor.hasNext();) {
					String guid = (String) guidItor.next();
				
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_SCAN_LOG WHERE CUSTOMER = ? AND CHARGE_ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, guid);
					stmt.executeUpdate();
					stmt.close();			
					
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND ITEM_GUID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, guid);
					stmt.executeUpdate();
					stmt.close();
				
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND ITEM_GUID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, guid);
					stmt.executeUpdate();
					stmt.close();

					stmt = conn.prepareStatement("DELETE FROM PARSCAN_STOCK_LOG WHERE CUSTOMER = ? AND ITEM_GUID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, guid);
					stmt.executeUpdate();
					stmt.close();
				
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND CHARGE_ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, guid);
					stmt.executeUpdate();
					stmt.close();
				
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_GUID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, guid);
					stmt.executeUpdate();
					stmt.close();	
				}
				guids.clear();				
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void removeResidentPayorCode(String customer, String residentGUID, List payorList)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for (Iterator itor = payorList.iterator(); itor.hasNext();) {
				ParScanPayorCodeBean data = (ParScanPayorCodeBean) itor.next();
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_PAYORINFO WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, residentGUID);
				stmt.setString(3, data.getID());
				stmt.executeUpdate();
				stmt.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeResidentPayorCode: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}

	/**
	 * Business Method.
	 */
	public void removeVendors(String customer, List vendorList)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtMedline;
		Connection conn = null;

		try {
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();

			for (Iterator itor = vendorList.iterator(); itor.hasNext();) {
				ParScanVendorBean data = (ParScanVendorBean) itor.next();
				
				stmtMedline = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
				stmtMedline.setString(1, customer);
				stmtMedline.setString(2, data.getVendorGUID());
				ResultSet rs = stmtMedline.executeQuery();
				rs.next();
				
				if(!rs.getString("VENDOR_NAME").equalsIgnoreCase("Medline Industries")){
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, data.getVendorGUID());
					stmt.executeUpdate();
					stmt.close();
				
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND VENDOR_GUID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, data.getVendorGUID());
					stmt.executeUpdate();
					stmt.close();				
				
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND VENDOR_GUID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, data.getVendorGUID());
					stmt.executeUpdate();
					stmt.close();		
					
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND VENDOR_ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, data.getVendorGUID());
					stmt.executeUpdate();
					stmt.close();		
				}
				
				stmtMedline.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeVednors: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void removePayorCodes(String customer, List payerList)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();

			for (Iterator itor = payerList.iterator(); itor.hasNext();) {
				ParScanPayorCodeBean data = (ParScanPayorCodeBean) itor.next();
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getPayorCode());
				stmt.executeUpdate();
				stmt.close();
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_PAYORINFO WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getPayorCode());
				stmt.executeUpdate();
				stmt.close();				
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removePayorCodes: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void updateVendor(String customer, ParScanVendorBean vendorList)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();

			stmt =
				conn.prepareStatement(
					"UPDATE PARSCAN_VENDOR SET VENDOR_NAME = ?, NOTES =?, PARSCAN_USER= ?, VENDOR_ID = ?, ADDRESS = ?, CITY = ?, VENDOR_STATE = ?, ZIP_CODE = ?, PHONE = ?, FAX = ? WHERE CUSTOMER = ? AND ID = ?");

			stmt.setString(1, vendorList.getVendorName());
			stmt.setString(2, emptyString(vendorList.getNotes()));
			stmt.setString(3, user);
			stmt.setString(4, vendorList.getVendorID());
			stmt.setString(5, emptyString(vendorList.getAddress()));
			stmt.setString(6, emptyString(vendorList.getCity()));
			stmt.setString(7, emptyString(vendorList.getState()));
			stmt.setString(8, emptyString(vendorList.getZipcode()));
			stmt.setString(9, emptyString(vendorList.getPhone()));
			stmt.setString(10, emptyString(vendorList.getFax()));
			stmt.setString(11, customer);
			stmt.setString(12, vendorList.getVendorGUID());

			stmt.executeUpdate();
			stmt.close();
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateVendor: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void updateCharge(String customer, ParScanResidentChargeBean charge)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy h:mm:ss aa");

		try {
			conn = openConnection();
			Timestamp sqlChargeDate = new Timestamp(dateTimeFormat.parse(charge.getStartDate()).getTime());

			stmt = conn.prepareStatement("UPDATE PARSCAN_RESIDENT SET QUANTITY = ?, CHARGE_DATE = ? WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, charge.getQuantity());
			stmt.setTimestamp(2, sqlChargeDate);
			stmt.setString(3, customer);
			stmt.setString(4, charge.getChargeID());

			stmt.executeUpdate();
			stmt.close();
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateCharge: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void updatePayorCode(String customer, String payorCode, ParScanPayorCodeBean payerList)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("UPDATE PARSCAN_PAYORINFO SET PAYOR_CODE = ? WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
			stmt.setString(1, payerList.getPayorCode());
			stmt.setString(2, customer);
			stmt.setString(3, payorCode);
			stmt.executeUpdate();
			stmt.close();

			stmt = conn.prepareStatement("UPDATE PARSCAN_PAYORCODES SET PAYOR_CODE = ?, DESCRIPTION = ?, NOTES =?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
			stmt.setString(1, payerList.getPayorCode());
			stmt.setString(2, payerList.getPayorDescription());
			stmt.setString(3, emptyString(payerList.getNotes()));
			stmt.setString(4, user);
			stmt.setString(5, customer);
			stmt.setString(6, payorCode);
			stmt.executeUpdate();
			stmt.close();
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updatePayorCode: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void updateService(
		String customer,
		ParScanServiceBean data, boolean priceChange)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtResident;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();
			
			Timestamp sqlPriceChangeDate = new Timestamp(System.currentTimeMillis());
			
			if(priceChange){
				stmt = conn.prepareStatement("UPDATE PARSCAN_SERVICES SET SERVICE = ?, DESCRIPTION = ?, FREQUENCY =?, PRICE =?, PARSCAN_USER= ?, PRICE_CHANGE_DATE = ? WHERE CUSTOMER = ? AND ID = ?");

				stmt.setString(1, data.getServiceName());
				stmt.setString(2, data.getDescription());
				stmt.setString(3, data.getFrequency());
				stmt.setString(4, data.getPrice());
				stmt.setString(5, user);
				stmt.setTimestamp(6, sqlPriceChangeDate);
				stmt.setString(7, customer);
				stmt.setString(8, data.getServiceGUID());

				stmt.executeUpdate();
				stmt.close();				
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CHARGE_ID = ? AND CUSTOMER = ? AND RECURRING_FLAG = 'X'");
				stmt.setString(1, data.getServiceGUID());
				stmt.setString(2, customer);
				
				ResultSet rs = stmt.executeQuery();
				
				while(rs.next()){
					DateTime today = new DateTime(new java.util.Date().getTime());
					DateTime startDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("START_DATE"))).getTime());
					DateTime endDate = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())).getTime());
					
					if(startDate.isAfter(today)){
						stmtResident = conn.prepareStatement("UPDATE PARSCAN_RESIDENT PRICE = ? WHERE CHARGE_ID = ? AND CUSTOMER = ? AND RECURRING_FLAG = 'X'");
						stmtResident.setString(1, data.getPrice());
						stmt.setString(2, data.getServiceGUID());
						stmt.setString(3, customer);
						stmtResident.executeUpdate();
						stmtResident.close();	
					}else if(endDate.isAfter(today) || endDate.equals(today)){
						stmtResident = conn.prepareStatement("UPDATE PARSCAN_RESIDENTS SET END_DATE = ? WHERE CUSTOMER = ? AND ID = ?");
						stmtResident.setTimestamp(1, sqlPriceChangeDate);
						stmtResident.setString(2, customer);
						stmtResident.setString(3, rs.getString("ID"));
						stmtResident.executeUpdate();
						stmtResident.close();
											
						stmtResident = conn.prepareStatement("INSERT INTO PARSCAN_RESIDENT (ID, CUSTOMER, RESIDENT_GUID, CHARGE_ID, SERVICE_FLAG, QUANTITY, PRICE, START_DATE, END_DATE, RECURRING_FLAG, CHARGE_DATE, PARSCAN_USER, FREQUENCY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");						
						UID uid = new UID();
						stmtResident.setString(1, uid.toString());
						stmtResident.setString(2, customer);
						stmtResident.setString(3, rs.getString("RESIDENT_GUID"));
						stmtResident.setString(4, data.getServiceGUID());
						stmtResident.setString(5, rs.getString("SERVICE_FLAG"));
						stmtResident.setString(6, rs.getString("QUANTITY"));
						stmtResident.setString(7, data.getPrice());
						stmtResident.setTimestamp(8, sqlPriceChangeDate);
						stmtResident.setTimestamp(9, rs.getTimestamp("END_DATE"));
						stmtResident.setString(10, rs.getString("RECURRING_FLAG"));
						stmtResident.setTimestamp(11, sqlPriceChangeDate);
						stmtResident.setString(12, user);
						stmtResident.setString(13, rs.getString("FREQUENCY"));
						stmtResident.executeUpdate();
						stmtResident.close();	
					}					
				}
				stmt.close();
			}else{
				stmt =
					conn.prepareStatement(
						"UPDATE PARSCAN_SERVICES SET SERVICE = ?, DESCRIPTION = ?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ID = ?");

				stmt.setString(1, data.getServiceName());
				stmt.setString(2, data.getDescription());
				stmt.setString(3, user);
				stmt.setString(4, customer);
				stmt.setString(5, data.getServiceGUID());

				stmt.executeUpdate();
				stmt.close();										
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateService: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void removeServices(String customer, List serviceList)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();
			
			for (Iterator itor = serviceList.iterator(); itor.hasNext();) {
				ParScanServiceBean data = (ParScanServiceBean) itor.next();

				stmt = conn.prepareStatement("DELETE FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND CHARGE_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getServiceGUID());
				stmt.executeUpdate();
				stmt.close();

				stmt = conn.prepareStatement("DELETE FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getServiceGUID());
				stmt.executeUpdate();
				stmt.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeServices: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void addService(String customer, ParScanServiceBean service)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		Random randomGenerator = new Random();
		boolean found = true;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			String barcode = "";
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND SERVICE = ?");
			stmt.setString(1, customer);
			stmt.setString(2, service.getServiceName());

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") <= 0) {				
				long randomInt = randomGenerator.nextInt(1000000000) * System.currentTimeMillis();			
				barcode = "S" + Long.toString(randomInt).substring(0,9);

				while(found){
					stmt.close();
					stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND SERVICE_BARCODE = ?");
					stmt.setString(1, customer);
					stmt.setString(2, barcode);
					rs = stmt.executeQuery();
					rs.next();

					if (rs.getInt("CNT") > 0) {
						randomInt = randomGenerator.nextInt(1000000000) * System.currentTimeMillis();			
						barcode = "S" + Long.toString(randomInt).substring(0,9);
					}else{
						found = false;					
					}
				}
				
				Timestamp sqlPriceChangeDate = new Timestamp(System.currentTimeMillis());				
				stmt.close();
				if(!service.getRelatedItem().equalsIgnoreCase("NA")){
					stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, service.getRelatedItem());
					rs = stmt.executeQuery();
					rs.next();

					if (rs.getInt("CNT") <= 0)
						service.setRelatedItem("NA");
					stmt.close();					
				}	
				
				if(!service.getFrequency().equalsIgnoreCase("Per Item/Service") && !service.getFrequency().equalsIgnoreCase("Daily") && !service.getFrequency().equalsIgnoreCase("Weekly") && !service.getFrequency().equalsIgnoreCase("Monthly") && !service.getFrequency().equalsIgnoreCase("Yearly"))
					service.setFrequency("Per Item/Service");
				
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_SERVICES (ID, CUSTOMER, SERVICE, DESCRIPTION, PRICE, FREQUENCY, ITEM_GUID, PARSCAN_USER, PRICE_CHANGE_DATE, SERVICE_BARCODE, CATEGORY_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();

				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, service.getServiceName());
				stmt.setString(4, service.getDescription());
				stmt.setString(5, service.getPrice());
				stmt.setString(6, service.getFrequency());
				stmt.setString(7, service.getRelatedItem());
				stmt.setString(8, user);
				stmt.setTimestamp(9, sqlPriceChangeDate);
				stmt.setString(10, barcode);
				stmt.setString(11, null);

				stmt.executeUpdate();
				stmt.close();
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addService: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getProductCategories(String customer, String PayorCode)
		throws ParScanResidentsEJBException {
		List categoryList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtCategory;
		Connection conn = null;

		try {
			conn = openConnection();

			if (PayorCode.equalsIgnoreCase("")) {
				stmt =
					conn.prepareStatement(
						"SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?");
				stmt.setString(1, customer);

				ResultSet rs = stmt.executeQuery();

				while (rs.next()) {
					ParScanCategoryBean data = new ParScanCategoryBean();

					data.setCategoryGUID(rs.getString("ID"));
					data.setProductCategory(rs.getString("PRODUCT_CATEGORY"));
					data.setARCode(rs.getString("AR_CODE"));
					if(rs.getString("EXCLUDE_FLAG") == null)
						data.setExcludeFlag("");
					else
						data.setExcludeFlag("1");
					if(rs.getString("OVERRIDE_FLAG") == null)
						data.setOverrideFlag("");
					else
						data.setOverrideFlag("1");						

					categoryList.add(data);
				}
				stmt.close();
			} else {
				stmt =
					conn.prepareStatement(
						"SELECT CATEGORY_ID FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
				stmt.setString(1, customer);
				stmt.setString(2, PayorCode);

				ResultSet rs = stmt.executeQuery();

				while (rs.next()) {
					if(rs.getString("CATEGORY_ID") != null){
						stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");

						stmtCategory.setString(1, customer);
						stmtCategory.setString(2, rs.getString("CATEGORY_ID"));

						ResultSet rsCategory = stmtCategory.executeQuery();

						while (rsCategory.next()) {
							ParScanCategoryBean data = new ParScanCategoryBean();

							data.setCategoryGUID(rsCategory.getString("ID"));
							data.setProductCategory(rsCategory.getString("PRODUCT_CATEGORY"));
							data.setARCode(rsCategory.getString("AR_CODE"));
							if(rs.getString("EXCLUDE_FLAG") == null)
								data.setExcludeFlag("");
							else
								data.setExcludeFlag("1");
							if(rs.getString("OVERRIDE_FLAG") == null)
								data.setOverrideFlag("");
							else
								data.setOverrideFlag("1");	
								
							categoryList.add(data);
						}

						stmtCategory.close();
					}
				}
				stmt.close();
			}

			return categoryList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getProductCategories: "
					+ stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void updateCategoryRelationship(String customer, ParScanPayorCodeBean PayorCode, List categoryList)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtCategory;
		PreparedStatement stmtUpdate;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("DELETE FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ? AND PAYOR_CODE = ?");

			stmt.setString(1, customer);
			stmt.setString(2, PayorCode.getPayorCode());
			
			stmt.executeUpdate();
			stmt.close();

			if(categoryList.size() > 0){
				for (Iterator itor = categoryList.iterator(); itor.hasNext();) {
					ParScanCategoryBean data = (ParScanCategoryBean) itor.next();
				
					stmtCategory = conn.prepareStatement("SELECT ID FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND PRODUCT_CATEGORY = ?");

					stmtCategory.setString(1, customer);
					stmtCategory.setString(2, data.getProductCategory());		
				
					ResultSet rs = stmtCategory.executeQuery();
					rs.next();
				
					stmt = conn.prepareStatement("INSERT INTO PARSCAN_PAYORCODES (ID, CUSTOMER, PAYOR_CODE, DESCRIPTION, NOTES, CATEGORY_ID, PARSCAN_USER) VALUES (?, ?, ?, ?, ?, ?, ?)");
					UID uid = new UID();

					stmt.setString(1, uid.toString());
					stmt.setString(2, customer);
					stmt.setString(3, PayorCode.getPayorCode());
					stmt.setString(4, PayorCode.getPayorDescription());
					stmt.setString(5, emptyString(PayorCode.getNotes()));
					stmt.setString(6, rs.getString("ID"));
					stmt.setString(7, user);

					stmt.executeUpdate();
					stmt.close();
					stmtCategory.close();						
				}	
			}		

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateCategoryRelationship: "
					+ stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getItemSearchList(String customerNumber, String id, String item, String description, String vendor, String category)throws ParScanResidentsEJBException {
		logger.entering("getItemSearchList()");
		List itemList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtMulti;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		String query = sqlProp.getString( "getItems"); 

		try {
			conn = openConnection();
			
			if(!id.equalsIgnoreCase(""))			
				query = query + " AND pi.ITEM_ID LIKE ?";
			if(!item.equalsIgnoreCase(""))				
				query = query + " AND pi.VENDOR_ITEM LIKE ?";
			if(!description.equalsIgnoreCase(""))
				query = query + " AND pi.DESCRIPTION LIKE ?";
			if(!vendor.equalsIgnoreCase("")){			
				query = query + " AND pi.VENDOR_ID = '"+vendor+"'";
			}				
			if(!category.equalsIgnoreCase("")){				
				query = query + " AND pi.CATEGORY_ID = '"+category+"'";
			}
			query = query + " ORDER BY ITEM_ID";

			int stmtCount = 3;
			stmt = conn.prepareStatement(query);			
			stmt.setString(1, customerNumber);
			stmt.setInt(2, 1);
			if(!id.equalsIgnoreCase("")){
				stmt.setString(stmtCount,"%"+id+"%");
				stmtCount++;			
			}
			if(!item.equalsIgnoreCase("")){
				stmt.setString(stmtCount,"%"+item+"%");
				stmtCount++;				
			}
			if(!description.equalsIgnoreCase("")){
				stmt.setString(stmtCount,"%"+description+"%");
				stmtCount++;				
			}		

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				ParScanItemBean data = new ParScanItemBean();
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
				stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?  AND ID = ?");
				String strVendor = vendor;
				String strCategory = category;
				
				if(rs.getString("VENDOR_ID")!= null){					
					stmtVendor.setString(1, customerNumber);
					stmtVendor.setString(2, rs.getString("VENDOR_ID"));
					ResultSet rsVendor = stmtVendor.executeQuery();
					rsVendor.next();
					strVendor = rsVendor.getString("VENDOR_NAME");
				}

				if(rs.getString("CATEGORY_ID")!= null){
					stmtCategory.setString(1, customerNumber);
					stmtCategory.setString(2, rs.getString("CATEGORY_ID"));
					ResultSet rsCategory = stmtCategory.executeQuery();
					rsCategory.next();
					strCategory = rsCategory.getString("PRODUCT_CATEGORY");
				}

				stmtMulti = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
				stmtMulti.setString(1, customerNumber);
				stmtMulti.setString(2, rs.getString("ITEM_ID"));				

				ResultSet rsMulti = stmtMulti.executeQuery();
				rsMulti.next();
				if (rsMulti.getInt("CNT") > 1){
					data.setMultilpleVendors(true);				
				}else{
					data.setMultilpleVendors(false);
				}

				data.setItemGUID(rs.getString("ID"));
				data.setItemID(rs.getString("ITEM_ID"));
				data.setItem(rs.getString("VENDOR_ITEM"));
				data.setDescription(rs.getString("DESCRIPTION"));
				data.setVendor(strVendor);
				data.setCategory(strCategory);
				data.setVendorUom(rs.getString("VENDOR_UOM"));
				data.setBillUom(rs.getString("BILL_UOM"));
				data.setCasePackaging(rs.getString("CASE_PACKAGING"));
				data.setCurrentCost(rs.getString("CURRENT_COST"));
				data.setFutureCost(rs.getString("FUTURE_COST"));
				if(rs.getTimestamp("FUTURE_COST_DATE") != null)
					data.setFutureCostEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_COST_DATE")));
				else
					data.setFutureCostEffectiveDate("");
				data.setCurrentPrice(rs.getString("CURRENT_PRICE"));
				data.setFuturePrice(rs.getString("FUTURE_PRICE"));
				if(rs.getTimestamp("FUTURE_PRICE_DATE") != null)
					data.setFuturePriceEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_PRICE_DATE")));
				else
					data.setFuturePriceEffectiveDate("");				
				data.setMultiplier(rs.getString("MULTIPLIER"));
				data.setAlternateBarcode(rs.getString("ALTERNATE_BARCODE"));
				data.setMedlineItem(rs.getString("MEDLINE_ITEM"));
				if(rs.getTimestamp("PRICE_CHANGE_DATE") != null)
					data.setPriceChangeDate(dateFormat.format(rs.getTimestamp("PRICE_CHANGE_DATE")));
				else
					data.setPriceChangeDate("");	
				if(rs.getTimestamp("COST_CHANGE_DATE") != null)
					data.setCostChangeDate(dateFormat.format(rs.getTimestamp("COST_CHANGE_DATE")));
				else
					data.setCostChangeDate("");	
				
				if(rs.getString("KIT_FLAG") != null)
					data.setKitFlag(true);					
				else
					data.setKitFlag(false);

				if( FORMULARY_YES_FLAG.equalsIgnoreCase( rs.getString("FORMULARY_FLAG")))
				{
					data.setFormularyFlag(true);	
				}
				else
				{
					data.setFormularyFlag(false);
				}
				data.setContractFlag("Y".equalsIgnoreCase(rs.getString("CONTRACT_FLAG")));	
				itemList.add(data);
				stmtVendor.close();
				stmtCategory.close();
				stmtMulti.close();
			}
			stmt.close();

			return itemList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getItemList: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
				logger.exiting("getItemSearchList()");
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public List getItemList(String customerNumber)throws ParScanResidentsEJBException {
		logger.entering(" getItemList()");
		List itemList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtMulti;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND VENDOR_PREFERENCE = ? ORDER BY ITEM_ID");
			stmt.setString(1, customerNumber);
			stmt.setInt(2, 1);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				ParScanItemBean data = new ParScanItemBean();
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
				stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?  AND ID = ?");
				String vendor = "";
				String category = "";
				
				if(rs.getString("VENDOR_ID")!= null){					
					stmtVendor.setString(1, customerNumber);
					stmtVendor.setString(2, rs.getString("VENDOR_ID"));
					ResultSet rsVendor = stmtVendor.executeQuery();
					rsVendor.next();
					vendor = rsVendor.getString("VENDOR_NAME");
				}

				if(rs.getString("CATEGORY_ID")!= null){
					stmtCategory.setString(1, customerNumber);
					stmtCategory.setString(2, rs.getString("CATEGORY_ID"));
					ResultSet rsCategory = stmtCategory.executeQuery();
					rsCategory.next();
					category = rsCategory.getString("PRODUCT_CATEGORY");
				}

				stmtMulti = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
				stmtMulti.setString(1, customerNumber);
				stmtMulti.setString(2, rs.getString("ITEM_ID"));				

				ResultSet rsMulti = stmtMulti.executeQuery();
				rsMulti.next();
				if (rsMulti.getInt("CNT") > 1){
					data.setMultilpleVendors(true);				
				}else{
					data.setMultilpleVendors(false);
				}

				data.setItemGUID(rs.getString("ID"));
				data.setItemID(rs.getString("ITEM_ID"));
				data.setItem(rs.getString("VENDOR_ITEM"));
				data.setDescription(rs.getString("DESCRIPTION"));
				data.setVendor(vendor);
				data.setCategory(category);
				data.setVendorUom(rs.getString("VENDOR_UOM"));
				data.setBillUom(rs.getString("BILL_UOM"));
				data.setCasePackaging(rs.getString("CASE_PACKAGING"));
				data.setCurrentCost(rs.getString("CURRENT_COST"));
				data.setFutureCost(rs.getString("FUTURE_COST"));
				if(rs.getTimestamp("FUTURE_COST_DATE") != null)
					data.setFutureCostEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_COST_DATE")));
				else
					data.setFutureCostEffectiveDate("");
				data.setCurrentPrice(rs.getString("CURRENT_PRICE"));
				data.setFuturePrice(rs.getString("FUTURE_PRICE"));
				if(rs.getTimestamp("FUTURE_PRICE_DATE") != null)
					data.setFuturePriceEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_PRICE_DATE")));
				else
					data.setFuturePriceEffectiveDate("");				
				data.setMultiplier(rs.getString("MULTIPLIER"));
				data.setAlternateBarcode(rs.getString("ALTERNATE_BARCODE"));
				data.setMedlineItem(rs.getString("MEDLINE_ITEM"));
				if(rs.getTimestamp("PRICE_CHANGE_DATE") != null)
					data.setPriceChangeDate(dateFormat.format(rs.getTimestamp("PRICE_CHANGE_DATE")));
				else
					data.setPriceChangeDate("");	
				if(rs.getTimestamp("COST_CHANGE_DATE") != null)
					data.setCostChangeDate(dateFormat.format(rs.getTimestamp("COST_CHANGE_DATE")));
				else
					data.setCostChangeDate("");	
				
				if(rs.getString("KIT_FLAG") != null)
					data.setKitFlag(true);					
				else
					data.setKitFlag(false);

				itemList.add(data);
				stmtVendor.close();
				stmtCategory.close();
				stmtMulti.close();
			}
			stmt.close();

			return itemList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getItemList: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
			logger.exiting( "getItemList()" );
		}
	}

	/**
	 * Business Method.
	 */
	public List getMultipleVendorProducts(String customer, List itemIDList)throws ParScanResidentsEJBException{
		List itemList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();

			for(Iterator itor = itemIDList.iterator();itor.hasNext();){
				ParScanItemBean idData = (ParScanItemBean) itor.next();

				stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, idData.getItemID());	
				ResultSet rs = stmt.executeQuery();
				rs.next();
				if(rs.getInt("CNT") > 1){
					stmt.close();
					
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ? ORDER BY ITEM_ID");
					stmt.setString(1, customer);
					stmt.setString(2, idData.getItemID());
	
					rs = stmt.executeQuery();
	
					while (rs.next()) {
						String category = "";
						stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?  AND ID = ?");
						ParScanItemBean data = new ParScanItemBean();
					
						if(rs.getString("CATEGORY_ID")!= null){
							stmtCategory.setString(1, customer);
							stmtCategory.setString(2, rs.getString("CATEGORY_ID"));
							ResultSet rsCategory = stmtCategory.executeQuery();
							rsCategory.next();
							category = rsCategory.getString("PRODUCT_CATEGORY");
							stmtCategory.close();
						}					
					
						data.setVendor("");
						if(rs.getString("VENDOR_ID") != null){
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
							stmtVendor.setString(1, customer);
							stmtVendor.setString(2, rs.getString("VENDOR_ID"));
							ResultSet rsVendor = stmtVendor.executeQuery();
							if(rsVendor.next())
								data.setVendor(rsVendor.getString("VENDOR_NAME"));
							stmtVendor.close();	
						}

						data.setItemGUID(rs.getString("ID"));
						data.setItemID(idData.getItemID());
						data.setItem(rs.getString("VENDOR_ITEM"));
						data.setDescription(rs.getString("DESCRIPTION"));
						data.setVendorUom(rs.getString("VENDOR_UOM"));
						data.setCasePackaging(rs.getString("CASE_PACKAGING"));
						data.setCurrentCost(rs.getString("CURRENT_COST"));
						data.setFutureCost(rs.getString("FUTURE_COST"));
						if(rs.getTimestamp("FUTURE_COST_DATE") != null)
							data.setFutureCostEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_COST_DATE")));
						else
							data.setFutureCostEffectiveDate("");
						data.setMultiplier(rs.getString("MULTIPLIER"));
						data.setVendorPreference(Integer.toString(rs.getInt("VENDOR_PREFERENCE")));
						data.setCategory(category);
						data.setBillUom(rs.getString("BILL_UOM"));
						data.setCasePackaging(rs.getString("CASE_PACKAGING"));
						data.setCurrentPrice(rs.getString("CURRENT_PRICE"));
						data.setFuturePrice(rs.getString("FUTURE_PRICE"));
						if(rs.getTimestamp("FUTURE_PRICE_DATE") != null)
							data.setFuturePriceEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_PRICE_DATE")));
						else
							data.setFuturePriceEffectiveDate("");				
						data.setAlternateBarcode(rs.getString("ALTERNATE_BARCODE"));	
										
						if( FORMULARY_YES_FLAG.equalsIgnoreCase( rs.getString("FORMULARY_FLAG")))
						{
							data.setFormularyFlag( true );
						}
						else
						{
							data.setFormularyFlag( false );
						}
						data.setContractFlag("Y".equalsIgnoreCase(rs.getString("CONTRACT_FLAG")));
						itemList.add(data);										
					}						
				}
	
				stmt.close();
			}
			
			return itemList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getMultipleVendorProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}

	/**
	 * Business Method.
	 */
	public List getKitDetails(String customer, String itemID)throws ParScanResidentsEJBException{
		List itemList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtItem;
		Connection conn = null;

		try {
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND KIT_ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, itemID);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1, customer);
				stmtItem.setString(2, rs.getString("ITEM_GUID"));
				ResultSet rsItem = stmtItem.executeQuery();
				
				while(rsItem.next()){
					double multiplier = Double.parseDouble(rsItem.getString("MULTIPLIER"));
					double cost = Double.parseDouble(rsItem.getString("CURRENT_COST"));
					
					ParScanItemBean data = new ParScanItemBean();						
					data.setVendor("");					
					if(rsItem.getString("VENDOR_ID") != null){
						stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
						stmtVendor.setString(1, customer);
						stmtVendor.setString(2, rsItem.getString("VENDOR_ID"));
						ResultSet rsVendor = stmtVendor.executeQuery();
						if(rsVendor.next())
							data.setVendor(rsVendor.getString("VENDOR_NAME"));	
						stmtVendor.close();											
					}

					data.setItemGUID(rsItem.getString("ID"));
					data.setItemID(rsItem.getString("ITEM_ID"));
					data.setItem(rsItem.getString("VENDOR_ITEM"));
					data.setDescription(rsItem.getString("DESCRIPTION"));						
					data.setBillUom(rsItem.getString("BILL_UOM"));
					data.setCasePackaging(rsItem.getString("CASE_PACKAGING"));
					data.setCurrentCost(rsItem.getString("CURRENT_COST"));
					data.setKitQuantity(rs.getString("KIT_QUANTITY"));
					data.setKitCost(Double.toString(cost/multiplier));
					data.setKitTotal(Double.toString(Double.parseDouble(data.getKitCost())*Double.parseDouble(data.getKitQuantity())));					
					
					itemList.add(data);									
				}
				stmtItem.close();
			}
			stmt.close();
			
			return itemList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getKitDetails: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}

	/**
	 * Business Method.
	 */
	public List getKitItems(String customer, String itemID)throws ParScanResidentsEJBException{
		List itemList = new ArrayList();
		PreparedStatement stmtVendor;
		PreparedStatement stmtItem;
		PreparedStatement stmt;
		Connection conn = null;

		try {
			if(itemID.equalsIgnoreCase("X")){
				conn = openConnection();
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? ORDER BY ITEM_ID");
				stmtItem.setString(1, customer);
				ResultSet rsItem = stmtItem.executeQuery();
			
				while(rsItem.next()){
					if(rsItem.getString("KIT_FLAG") == null){
						ParScanItemBean data = new ParScanItemBean();						
						data.setVendor("");					
						if(rsItem.getString("VENDOR_ID") != null){
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
							stmtVendor.setString(1, customer);
							stmtVendor.setString(2, rsItem.getString("VENDOR_ID"));
							ResultSet rsVendor = stmtVendor.executeQuery();
							if(rsVendor.next())
								data.setVendor(rsVendor.getString("VENDOR_NAME"));	
							stmtVendor.close();											
						}

						data.setItemGUID(rsItem.getString("ID"));
						data.setItemID(rsItem.getString("ITEM_ID"));
						data.setItem(rsItem.getString("VENDOR_ITEM"));
						data.setDescription(rsItem.getString("DESCRIPTION"));						
						data.setVendorUom(rsItem.getString("VENDOR_UOM"));
						data.setBillUom(rsItem.getString("BILL_UOM"));
						data.setCasePackaging(rsItem.getString("CASE_PACKAGING"));
						data.setCurrentCost(rsItem.getString("CURRENT_COST"));
						data.setMultiplier(rsItem.getString("MULTIPLIER"));							
				
						itemList.add(data);
					}									
				}
				stmtItem.close();

			}else{
				conn = openConnection();				
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? ORDER BY ITEM_ID");
				stmtItem.setString(1, customer);
				ResultSet rsItem = stmtItem.executeQuery();
			
				while(rsItem.next()){
					stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_KIT WHERE CUSTOMER = ?  AND KIT_ID = ? AND ITEM_GUID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, itemID);
					stmt.setString(3, rsItem.getString("ID"));
					ResultSet rs = stmt.executeQuery();

					rs.next();
				
					if(rs.getInt("CNT") <= 0){
						if(rsItem.getString("KIT_FLAG") == null){
							ParScanItemBean data = new ParScanItemBean();						
							data.setVendor("");					
							if(rsItem.getString("VENDOR_ID") != null){
								stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
								stmtVendor.setString(1, customer);
								stmtVendor.setString(2, rsItem.getString("VENDOR_ID"));
								ResultSet rsVendor = stmtVendor.executeQuery();
								if(rsVendor.next())
									data.setVendor(rsVendor.getString("VENDOR_NAME"));	
								stmtVendor.close();											
							}

							data.setItemGUID(rsItem.getString("ID"));
							data.setItemID(rsItem.getString("ITEM_ID"));
							data.setItem(rsItem.getString("VENDOR_ITEM"));
							data.setDescription(rsItem.getString("DESCRIPTION"));						
							data.setVendorUom(rsItem.getString("VENDOR_UOM"));
							data.setBillUom(rsItem.getString("BILL_UOM"));
							data.setCasePackaging(rsItem.getString("CASE_PACKAGING"));
							data.setCurrentCost(rsItem.getString("CURRENT_COST"));
							data.setMultiplier(rsItem.getString("MULTIPLIER"));							
				
							itemList.add(data);
						}													
					}	
					stmt.close();								
				}
				stmtItem.close();
				
			}
						
			return itemList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getKitItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}

	/**
	 * Business Method.
	 */
	public void addProductCategory(String customer, ParScanCategoryBean category)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND PRODUCT_CATEGORY = ?");
			stmt.setString(1, customer);
			stmt.setString(2, category.getProductCategory());

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") <= 0) {
				stmt.close();
				stmt =
					conn.prepareStatement(
						"INSERT INTO PARSCAN_CATEGORY (ID, CUSTOMER, PRODUCT_CATEGORY, AR_CODE, EXCLUDE_FLAG, OVERRIDE_FLAG, PARSCAN_USER) VALUES (?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();

				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, category.getProductCategory());
				stmt.setString(4, emptyString(category.getARCode()));
				stmt.setString(5, emptyString(category.getExcludeFlag()));
				stmt.setString(6, emptyString(category.getOverrideFlag()));
				stmt.setString(7, user);

				stmt.executeUpdate();
				stmt.close();
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addProductCategory: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void assignProductCategory(String customer, String categoryGUID, List productList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for (Iterator itor = productList.iterator(); itor.hasNext();) {
				ParScanItemBean data = (ParScanItemBean) itor.next();
				stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET CATEGORY_ID = ?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ITEM_ID = ?");
	
				stmt.setString(1, categoryGUID);
				stmt.setString(2, user);
				stmt.setString(3, customer);
				stmt.setString(4, data.getItemID());
	
				stmt.executeUpdate();
				stmt.close();
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during assignProductCategory: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void assignServiceCategory(String customer, String categoryGUID, List serviceList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();

			for (Iterator itor = serviceList.iterator(); itor.hasNext();) {
				ParScanServiceBean data = (ParScanServiceBean) itor.next();
				stmt = conn.prepareStatement("UPDATE PARSCAN_SERVICES SET CATEGORY_ID = ? WHERE CUSTOMER = ? AND ID = ?");
	
				stmt.setString(1, categoryGUID);
				stmt.setString(2, customer);
				stmt.setString(3, data.getServiceGUID());
	
				stmt.executeUpdate();
				stmt.close();
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during assignServiceCategory: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public String getServiceCategory(String customer, String categoryGUID)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtCat;
		Connection conn = null;
		String category = "";
		
		try {
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, categoryGUID);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				if(rs.getString("CATEGORY_ID") != null){
					stmtCat = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?  AND ID = ?");
					stmtCat.setString(1, customer);
					stmtCat.setString(2, rs.getString("CATEGORY_ID"));

					ResultSet rsCat = stmtCat.executeQuery();
				
					if(rsCat.next())
						category = rsCat.getString("PRODUCT_CATEGORY");
				}else
					category = "";
			}
			
			return category;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getServiceCategory: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}		
	
	/**
	 * Business Method.
	 */
	public void assignVendor(String customer, String vendorGUID, List productList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for (Iterator itor = productList.iterator(); itor.hasNext();) {
				ParScanItemBean data = (ParScanItemBean) itor.next();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET VENDOR_ID = ?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ID = ?");	
				stmt.setString(1, vendorGUID);
				stmt.setString(2, user);
				stmt.setString(3, customer);
				stmt.setString(4, data.getItemGUID());	
				stmt.executeUpdate();
				stmt.close();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET VENDOR_GUID = ?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ITEM_GUID = ?");	
				stmt.setString(1, vendorGUID);
				stmt.setString(2, user);
				stmt.setString(3, customer);
				stmt.setString(4, data.getItemGUID());	
				stmt.executeUpdate();
				stmt.close();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET VENDOR_GUID = ?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ITEM_GUID = ?");	
				stmt.setString(1, vendorGUID);
				stmt.setString(2, user);
				stmt.setString(3, customer);
				stmt.setString(4, data.getItemGUID());	
				stmt.executeUpdate();
				stmt.close();
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during assignVendor: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
		
	
	/**
	 * Business Method.
	 */
	public void removeProductCategory(String customer, List categoryList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;

		try {
			conn = openConnection();

			for (Iterator itor = categoryList.iterator(); itor.hasNext();) {
				ParScanCategoryBean data = (ParScanCategoryBean) itor.next();
				
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getCategoryGUID());
				stmt.executeUpdate();
				stmt.close();
				
				stmtItem = conn.prepareStatement("UPDATE PARSCAN_ITEM SET CATEGORY_ID = ? WHERE CUSTOMER = ? AND CATEGORY_ID = ?");				
				stmtItem.setString(1, null);
				stmtItem.setString(2, customer);
				stmtItem.setString(3, data.getCategoryGUID());
				stmtItem.executeUpdate();								
				stmtItem.close();
				
				stmtItem = conn.prepareStatement("UPDATE PARSCAN_SERVICES SET CATEGORY_ID = ? WHERE CUSTOMER = ? AND CATEGORY_ID = ?");				
				stmtItem.setString(1, null);
				stmtItem.setString(2, customer);
				stmtItem.setString(3, data.getCategoryGUID());
				stmtItem.executeUpdate();								
				stmtItem.close();				
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeProductCategory: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void toggleProductCategoryExclusion(String customer, List categoryList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();

			for (Iterator itor = categoryList.iterator(); itor.hasNext();) {
				ParScanCategoryBean data = (ParScanCategoryBean) itor.next();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_CATEGORY SET EXCLUDE_FLAG = ? WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, emptyString(data.getExcludeFlag()));
				stmt.setString(2, customer);
				stmt.setString(3, data.getCategoryGUID());
				stmt.executeUpdate();
				stmt.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during toggleProductCategoryExclusion: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void toggleProductCategoryOverride(String customer, List categoryList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();

			for (Iterator itor = categoryList.iterator(); itor.hasNext();) {
				ParScanCategoryBean data = (ParScanCategoryBean) itor.next();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_CATEGORY SET OVERRIDE_FLAG = ? WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, emptyString(data.getOverrideFlag()));
				stmt.setString(2, customer);
				stmt.setString(3, data.getCategoryGUID());
				stmt.executeUpdate();
				stmt.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during toggleProductCategoryOverride: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void saveProductCategoryChanges(String customer, ParScanCategoryBean category)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();
				
			stmt = conn.prepareStatement("UPDATE PARSCAN_CATEGORY SET PRODUCT_CATEGORY = ?, AR_CODE = ?, EXCLUDE_FLAG = ?, OVERRIDE_FLAG = ? WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, category.getProductCategory());
			stmt.setString(2, emptyString(category.getARCode()));
			stmt.setString(3, emptyString(category.getExcludeFlag()));
			stmt.setString(4, emptyString(category.getOverrideFlag()));
			stmt.setString(5, customer);
			stmt.setString(6, category.getCategoryGUID());

			stmt.executeUpdate();
			stmt.close();
				

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during saveProductCategoryChanges: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	private String emptyString(String str){
		if(str.equalsIgnoreCase("")){
			return null;
		}else{
			return str;
		}
	}
	
	/**
	 * Business Method.
	 */	
	public void addVendorProduct(String customer, boolean preferred, ParScanItemBean item)throws ParScanResidentsEJBException 
	{
		String methodName = "addVendorProduct()";
		logger.entering(methodName);
		
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtItem;
		Connection conn = null;
		Timestamp sqlFutureCostDate = null;
		Timestamp sqlPriceChangeDate = null;
		int vendorNumber = 0;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
	
		try {			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
	
			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, item.getItemID());
	
			ResultSet rs = stmt.executeQuery();
			rs.next();
			vendorNumber = rs.getInt("CNT"); 
			if (vendorNumber > 0) {
				stmtVendor = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND VENDOR_NAME = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, item.getVendor());
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				if(rsVendor.getInt("CNT") <= 0){
					ParScanVendorBean vendor = new ParScanVendorBean();
					vendor.setVendorID(item.getVendor());
					vendor.setVendorName(item.getVendor());
					vendor.setNotes("");
					vendor.setAddress("");
					vendor.setCity("");
					vendor.setFax("");
					vendor.setPhone("");
					vendor.setState("");
					vendor.setZipcode("");						
					addVendor(customer, vendor);
				}
				stmtVendor.close();
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, item.getVendor());
	
				rsVendor = stmtVendor.executeQuery();
				if(rsVendor.next()){
					if(rsVendor.getString("VENDOR_NAME").toUpperCase().indexOf("MEDLINE") != -1){
						if(productValidation(item.getItem())){
							item.setMedlineItem("X");
						}else{
							item.setMedlineItem(null);
						}
					}else{
						item.setMedlineItem(null);
					}
					
					stmtVendor.close();
				
					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
					stmtItem.setString(1, customer);
					stmtItem.setString(2, item.getItemID());
					ResultSet rsItem = stmtItem.executeQuery();				
					rsItem.next();
				
					stmt.close();
					stmt = conn.prepareStatement("INSERT INTO PARSCAN_ITEM (ID, CUSTOMER, ITEM_ID, VENDOR_ITEM, DESCRIPTION, VENDOR_ID, CATEGORY_ID, VENDOR_UOM, MULTIPLIER, CASE_PACKAGING, CURRENT_COST, FUTURE_COST, FUTURE_COST_DATE, CURRENT_PRICE, FUTURE_PRICE, FUTURE_PRICE_DATE, ALTERNATE_BARCODE, MEDLINE_ITEM, BILL_UOM, PARSCAN_USER, PRICE_CHANGE_DATE, VENDOR_PREFERENCE, COST_CHANGE_DATE, KIT_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					
					if(!item.getFutureCostEffectiveDate().equalsIgnoreCase(""))
						sqlFutureCostDate = new Timestamp(dateFormat.parse(item.getFutureCostEffectiveDate()).getTime());
					
					UID uid = new UID();
	
					stmt.setString(1, uid.toString());
					stmt.setString(2, customer);
					stmt.setString(3, rsItem.getString("ITEM_ID"));
					stmt.setString(4, emptyString(item.getItem()));
					stmt.setString(5, rsItem.getString("DESCRIPTION"));
					stmt.setString(6, emptyString(item.getVendor()));
					stmt.setString(7, rsItem.getString("CATEGORY_ID"));
					stmt.setString(8, emptyString(item.getVendorUom()));
					stmt.setString(9, emptyString(item.getMultiplier()));
					stmt.setString(10, item.getMultiplier() + " " + rsItem.getString("BILL_UOM") + "/" + item.getVendorUom());
					stmt.setString(11, emptyString(item.getCurrentCost()));
					stmt.setString(12, emptyString(item.getFutureCost()));
					stmt.setTimestamp(13, sqlFutureCostDate);
					stmt.setString(14, rsItem.getString("CURRENT_PRICE"));
					stmt.setString(15, rsItem.getString("FUTURE_PRICE"));				
					stmt.setTimestamp(16, rsItem.getTimestamp("FUTURE_PRICE_DATE"));
					stmt.setString(17, rsItem.getString("ALTERNATE_BARCODE"));
					stmt.setString(18, item.getMedlineItem());
					stmt.setString(19, rsItem.getString("BILL_UOM"));
					stmt.setString(20, user);
					stmt.setTimestamp(21, sqlPriceChangeDate);
					if(preferred){					
						stmt.setInt(22, 1);
					}else{
						stmt.setInt(22, vendorNumber + 1);
					}
					stmt.setTimestamp(23, sqlPriceChangeDate);
					stmt.setString(24, null);
				
					stmt.executeUpdate();
					stmt.close();
					stmtItem.close();
					if(preferred)
						updateVendorPreference(customer, rsItem.getString("ITEM_ID"), item.getVendor());
				}else{
					stmtVendor.close();
					stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND VENDOR_NAME = ?");
					stmtVendor.setString(1, customer);
					stmtVendor.setString(2, item.getVendor());
	
					rsVendor = stmtVendor.executeQuery();
					if(rsVendor.next()){
						if(rsVendor.getString("VENDOR_NAME").toUpperCase().indexOf("MEDLINE") != -1){
							if(productValidation(item.getItem())){
								item.setMedlineItem("X");
							}else{
								item.setMedlineItem(null);
							}
						}else{
							item.setMedlineItem(null);
						}
						item.setVendor(rsVendor.getString("ID"));					
						stmtVendor.close();
				
						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
						stmtItem.setString(1, customer);
						stmtItem.setString(2, item.getItemID());
						ResultSet rsItem = stmtItem.executeQuery();				
						rsItem.next();
				
						stmt.close();
						stmt = conn.prepareStatement("INSERT INTO PARSCAN_ITEM (ID, CUSTOMER, ITEM_ID, VENDOR_ITEM, DESCRIPTION, VENDOR_ID, CATEGORY_ID, VENDOR_UOM, MULTIPLIER, CASE_PACKAGING, CURRENT_COST, FUTURE_COST, FUTURE_COST_DATE, CURRENT_PRICE, FUTURE_PRICE, FUTURE_PRICE_DATE, ALTERNATE_BARCODE, MEDLINE_ITEM, BILL_UOM, PARSCAN_USER, PRICE_CHANGE_DATE, VENDOR_PREFERENCE, COST_CHANGE_DATE, KIT_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					
						if(!item.getFutureCostEffectiveDate().equalsIgnoreCase(""))
							sqlFutureCostDate = new Timestamp(dateFormat.parse(item.getFutureCostEffectiveDate()).getTime());
					
						UID uid = new UID();
	
						stmt.setString(1, uid.toString());
						stmt.setString(2, customer);
						stmt.setString(3, rsItem.getString("ITEM_ID"));
						stmt.setString(4, emptyString(item.getItem()));
						stmt.setString(5, rsItem.getString("DESCRIPTION"));
						stmt.setString(6, emptyString(item.getVendor()));
						stmt.setString(7, rsItem.getString("CATEGORY_ID"));
						stmt.setString(8, emptyString(item.getVendorUom()));
						stmt.setString(9, emptyString(item.getMultiplier()));
						stmt.setString(10, item.getMultiplier() + " " + rsItem.getString("BILL_UOM") + "/" + item.getVendorUom());
						stmt.setString(11, emptyString(item.getCurrentCost()));
						stmt.setString(12, emptyString(item.getFutureCost()));
						stmt.setTimestamp(13, sqlFutureCostDate);
						stmt.setString(14, rsItem.getString("CURRENT_PRICE"));
						stmt.setString(15, rsItem.getString("FUTURE_PRICE"));				
						stmt.setTimestamp(16, rsItem.getTimestamp("FUTURE_PRICE_DATE"));
						stmt.setString(17, rsItem.getString("ALTERNATE_BARCODE"));
						stmt.setString(18, item.getMedlineItem());
						stmt.setString(19, rsItem.getString("BILL_UOM"));
						stmt.setString(20, user);
						stmt.setTimestamp(21, sqlPriceChangeDate);
						if(preferred){					
							stmt.setInt(22, 1);
						}else{
							stmt.setInt(22, vendorNumber + 1);
						}
						stmt.setTimestamp(23, sqlPriceChangeDate);
						stmt.setString(24, null);
				
						stmt.executeUpdate();
						stmt.close();
						stmtItem.close();
						if(preferred)
							updateVendorPreference(customer, rsItem.getString("ITEM_ID"), item.getVendor());					
					}					
				}
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addVendorProduct: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
			
			logger.exiting(methodName);
		}
	}

	private void updateVendorPreference(String customer, String itemID, String vendorGUID)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;
	
		try {			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
	
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ? AND VENDOR_ID != ?");
			stmt.setString(1, customer);
			stmt.setString(2, itemID);
			stmt.setString(3, vendorGUID);
	
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				stmtItem = conn.prepareStatement("UPDATE PARSCAN_ITEM SET VENDOR_PREFERENCE = ? WHERE CUSTOMER = ? AND ITEM_ID = ? AND VENDOR_ID = ?");
				stmtItem.setInt(1, rs.getInt("VENDOR_PREFERENCE") + 1);
				stmtItem.setString(2, customer);
				stmtItem.setString(3, itemID);
				stmtItem.setString(4, vendorGUID);
				stmtItem.executeUpdate();
				stmtItem.close();
			}
			
			stmt.close();			
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateVendorPreference: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}
	
	/**
	 * Business Method.
	 */
	public void addProduct(String customer, ParScanItemBean item)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		Timestamp sqlFutureCostDate = null;
		Timestamp sqlFuturePriceDate = null;
		Timestamp sqlPriceChangeDate = null;
		Timestamp sqlCostChangeDate = null;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, item.getItemID());

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") <= 0) {
				item.setMedlineItem("");
				if(!item.getVendor().equalsIgnoreCase("")){
					stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
					stmtVendor.setString(1, customer);
					stmtVendor.setString(2, item.getVendor());
					ResultSet rsVendor = stmtVendor.executeQuery();
					rsVendor.next();								
					if(rsVendor.getString("VENDOR_NAME").toUpperCase().indexOf("MEDLINE") != -1){
						if(productValidation(item.getItem())){
							item.setMedlineItem("X");
						}
					}				
					stmtVendor.close();					
				}

				if(item.getMedlineItem().equalsIgnoreCase("X")){
					//Get Medline multiplier, case packaging, description (if blank), UOM, and cost
					if(item.getDescription().equalsIgnoreCase(""))
						item.setDescription(getDescription(item.getItem()));
					item.setVendorUom(getMedlineUOM(item.getItem()));
					if(item.getBillUom().equalsIgnoreCase(""))
						item.setBillUom("EA");
					if(item.getMultiplier().equalsIgnoreCase("")){
						item.setMultiplier(Integer.toString((int) getConversion(item.getVendorUom(),item.getItem())));					
						item.setCasePackaging(Integer.toString((int) getConversion(item.getVendorUom(),item.getItem())) + " EA/" + item.getVendorUom());	
					}else{
						item.setCasePackaging(item.getMultiplier() + " " + item.getBillUom() + "/" + item.getVendorUom());
					}
					item.setCurrentCost(getMedlinePrice(customer, item.getItem()));												
				}	

				stmt.close();
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_ITEM (ID, CUSTOMER, ITEM_ID, VENDOR_ITEM, DESCRIPTION, VENDOR_ID, CATEGORY_ID, VENDOR_UOM, MULTIPLIER, CASE_PACKAGING, CURRENT_COST, FUTURE_COST, FUTURE_COST_DATE, CURRENT_PRICE, FUTURE_PRICE, FUTURE_PRICE_DATE, ALTERNATE_BARCODE, MEDLINE_ITEM, BILL_UOM, PARSCAN_USER, PRICE_CHANGE_DATE, VENDOR_PREFERENCE, COST_CHANGE_DATE, KIT_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				
				if(!item.getFutureCostEffectiveDate().equalsIgnoreCase(""))
					sqlFutureCostDate = new Timestamp(dateFormat.parse(item.getFutureCostEffectiveDate()).getTime());
				if(!item.getFuturePriceEffectiveDate().equalsIgnoreCase(""))
					sqlFuturePriceDate = new Timestamp(dateFormat.parse(item.getFuturePriceEffectiveDate()).getTime());
				if(!item.getCurrentPrice().equalsIgnoreCase(""))				
					sqlPriceChangeDate = new Timestamp(new java.util.Date().getTime());
				if(!item.getCurrentCost().equalsIgnoreCase(""))
					sqlCostChangeDate = new Timestamp(new java.util.Date().getTime());					
				
				UID uid = new UID();

				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, item.getItemID().toUpperCase());
				stmt.setString(4, emptyString(item.getItem().toUpperCase()));
				stmt.setString(5, item.getDescription());
				stmt.setString(6, emptyString(item.getVendor()));
				stmt.setString(7, emptyString(item.getCategory()));
				stmt.setString(8, emptyString(item.getVendorUom()));
				stmt.setString(9, emptyString(item.getMultiplier()));
				stmt.setString(10, emptyString(item.getCasePackaging()));
				stmt.setString(11, emptyString(item.getCurrentCost()));
				stmt.setString(12, emptyString(item.getFutureCost()));
				stmt.setTimestamp(13, sqlFutureCostDate);
				stmt.setString(14, emptyString(item.getCurrentPrice()));
				stmt.setString(15, emptyString(item.getFuturePrice()));				
				stmt.setTimestamp(16, sqlFuturePriceDate);
				stmt.setString(17, emptyString(item.getAlternateBarcode()));
				stmt.setString(18, emptyString(item.getMedlineItem()));
				stmt.setString(19, emptyString(item.getBillUom()));
				stmt.setString(20, user);
				stmt.setTimestamp(21, sqlPriceChangeDate);
				stmt.setInt(22, 1);
				stmt.setTimestamp(23, sqlCostChangeDate);
				stmt.setString(24, null);

				stmt.executeUpdate();
				stmt.close();
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addProduct: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	

	/**
	 * Business Method.
	 */
	public void addKit(String customer, List productList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Timestamp sqlPriceChangeDate = null;
		Timestamp sqlCostChangeDate = null;
		Connection conn = null;

		try {			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
			boolean addProduct = true;
			for(Iterator itor = productList.iterator();itor.hasNext();){
				ParScanItemBean item = (ParScanItemBean) itor.next();
				
				if(addProduct){
					addProduct = false;

					stmt = conn.prepareStatement("INSERT INTO PARSCAN_ITEM (ID, CUSTOMER, ITEM_ID, VENDOR_ITEM, DESCRIPTION, VENDOR_ID, CATEGORY_ID, VENDOR_UOM, MULTIPLIER, CASE_PACKAGING, CURRENT_COST, FUTURE_COST, FUTURE_COST_DATE, CURRENT_PRICE, FUTURE_PRICE, FUTURE_PRICE_DATE, ALTERNATE_BARCODE, MEDLINE_ITEM, BILL_UOM, PARSCAN_USER, PRICE_CHANGE_DATE, VENDOR_PREFERENCE, COST_CHANGE_DATE, KIT_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				
					if(!item.getCurrentPrice().equalsIgnoreCase(""))				
						sqlPriceChangeDate = new Timestamp(new java.util.Date().getTime());
					if(!item.getCurrentCost().equalsIgnoreCase(""))
						sqlCostChangeDate = new Timestamp(new java.util.Date().getTime());					
				
					UID uid = new UID();
					stmt.setString(1, uid.toString());
					stmt.setString(2, customer);
					stmt.setString(3, item.getItemID().toUpperCase());
					stmt.setString(4, emptyString(item.getItem().toUpperCase()));
					stmt.setString(5, item.getDescription());
					stmt.setString(6, null);
					stmt.setString(7, null);
					stmt.setString(8, emptyString(item.getVendorUom()));
					stmt.setString(9, emptyString(item.getMultiplier()));
					stmt.setString(10, emptyString(item.getCasePackaging()));
					stmt.setString(11, emptyString(item.getCurrentCost()));
					stmt.setString(12,null);
					stmt.setTimestamp(13, null);
					stmt.setString(14, emptyString(item.getCurrentPrice()));
					stmt.setString(15, null);				
					stmt.setTimestamp(16, null);
					stmt.setString(17, emptyString(item.getAlternateBarcode()));
					stmt.setString(18, null);
					stmt.setString(19, emptyString(item.getBillUom()));
					stmt.setString(20, user);
					stmt.setTimestamp(21, sqlPriceChangeDate);
					stmt.setInt(22, 1);
					stmt.setTimestamp(23, sqlCostChangeDate);
					stmt.setString(24, "X");
					stmt.executeUpdate();
					stmt.close();
				}
				
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_KIT (ID, CUSTOMER, KIT_ID, ITEM_GUID, KIT_QUANTITY) VALUES (?, ?, ?, ?, ?)");				
				UID uid = new UID();
				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, item.getItemID().toUpperCase());
				stmt.setString(4, item.getItemGUID());
				stmt.setString(5, item.getKitQuantity());
				stmt.executeUpdate();
				stmt.close();								
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addKit: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void addToKit(String customer, List productList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
			for(Iterator itor = productList.iterator();itor.hasNext();){
				ParScanItemBean item = (ParScanItemBean) itor.next();
				
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_KIT (ID, CUSTOMER, KIT_ID, ITEM_GUID, KIT_QUANTITY) VALUES (?, ?, ?, ?, ?)");				
				UID uid = new UID();
				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, item.getItemID().toUpperCase());
				stmt.setString(4, item.getItemGUID());
				stmt.setString(5, item.getKitQuantity());
				stmt.executeUpdate();
				stmt.close();								
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addToKit: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void addProducts(String customer, List productList, String parArea, String venLevelUOM, String updateItem)throws ParScanResidentsEJBException 
	{
		String methodName = "addProducts()";
		logger.entering(methodName);
		
		logger.debugT("productList.size() = "+ productList.size() + ", updateItem = " + updateItem);
		
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;
		PreparedStatement stmtUOM;
		Timestamp sqlFutureCostDate = null;
		Timestamp sqlFuturePriceDate = null;
		Timestamp sqlPriceChangeDate = null;
		Timestamp sqlCostChangeDate = null;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for(Iterator itor = productList.iterator();itor.hasNext();){
				ParScanItemBean item = (ParScanItemBean) itor.next();

				logger.debugT("Cuurent product : " + item.getItemID());
				
				stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, item.getItemID().toUpperCase());

				ResultSet rs = stmt.executeQuery();
				rs.next();
				if (rs.getInt("CNT") <= 0){
					//ParScanResidentsSSBean.logger.debugT(item.getItemID().toUpperCase() + " not found");
					if(item.getVendor().toUpperCase().indexOf("MEDLINE") != -1){
						stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
						stmtVendor.setString(1, customer);
						stmtVendor.setString(2, "Medline Industries");
						ResultSet rsVendor = stmtVendor.executeQuery();
						rsVendor.next();
						item.setVendor(rsVendor.getString("ID"));
						stmtVendor.close();		
						if(productValidation(item.getItem())){
							item.setMedlineItem("X");
						}
					}else{
						boolean foundVendor = false;
						stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME <> 'Medline Industries'");
						stmtVendor.setString(1, customer);
						ResultSet rsVendor = stmtVendor.executeQuery();
						while(rsVendor.next()){
							if(rsVendor.getString("VENDOR_NAME").equalsIgnoreCase(item.getVendor())){
								foundVendor = true;
								item.setVendor(rsVendor.getString("ID"));						
								break;
							}
						}				
						stmtVendor.close();
						if(!foundVendor && !item.getVendor().equalsIgnoreCase("")){
							ParScanVendorBean vendorBean = new ParScanVendorBean();						
							vendorBean.setVendorName(item.getVendor());
							vendorBean.setVendorID(item.getVendor());	
							vendorBean.setNotes("");
							vendorBean.setAddress("");
							vendorBean.setCity("");
							vendorBean.setFax("");
							vendorBean.setPhone("");
							vendorBean.setState("");
							vendorBean.setZipcode("");													
							addVendor(customer, vendorBean);
						
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
							stmtVendor.setString(1, customer);
							stmtVendor.setString(2, item.getVendor());
							rsVendor = stmtVendor.executeQuery();
							rsVendor.next();
							item.setVendor(rsVendor.getString("ID"));
							stmtVendor.close();						
						}						
					}

					
					stmt.close();
					
					boolean foundCategory = false;
					stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?");
					stmtCategory.setString(1, customer);
					ResultSet rsCategory = stmtCategory.executeQuery();
					while(rsCategory.next()){
						if(rsCategory.getString("PRODUCT_CATEGORY").equalsIgnoreCase(item.getCategory())){
							foundCategory = true;
							item.setCategory(rsCategory.getString("ID"));						
							break;
						}
					}				
					stmtCategory.close();
					if(!foundCategory && !item.getCategory().equalsIgnoreCase("")){
						ParScanCategoryBean categoryBean = new ParScanCategoryBean();						
						categoryBean.setARCode("");
						categoryBean.setExcludeFlag("");
						categoryBean.setOverrideFlag("");
						categoryBean.setProductCategory(item.getCategory());						
						addProductCategory(customer, categoryBean);
						
						stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND PRODUCT_CATEGORY = ?");
						stmtCategory.setString(1, customer);
						stmtCategory.setString(2, item.getCategory());
						rsCategory = stmtCategory.executeQuery();
						rsCategory.next();
						item.setCategory(rsCategory.getString("ID"));
						stmtCategory.close();						
					}					
						
					ParScanUOMBean uom = new ParScanUOMBean();		
					if(!item.getBillUom().equalsIgnoreCase("")){
						uom.setUom(item.getBillUom());
						uom.setDescription(item.getBillUom());	
						addUOM(customer, uom);
					}
					if(!item.getVendorUom().equalsIgnoreCase("")){
						uom.setUom(item.getVendorUom());
						uom.setDescription(item.getVendorUom());
						addUOM(customer, uom);						
					}
						
					if(item.getMedlineItem().equalsIgnoreCase("X")){
						//Get Medline multiplier, case packaging, description (if blank), UOM, and cost
						if(item.getDescription().equalsIgnoreCase(""))
							item.setDescription(getDescription(item.getItem()));
						item.setVendorUom(getMedlineUOM(item.getItem()));
						if(item.getBillUom().equalsIgnoreCase(""))
							item.setBillUom("EA");
						if(item.getMultiplier().equalsIgnoreCase("")){
							item.setMultiplier(Integer.toString((int) getConversion(item.getVendorUom(),item.getItem())));					
							item.setCasePackaging(Integer.toString((int) getConversion(item.getVendorUom(),item.getItem())) + " EA/" + item.getVendorUom());	
						}else{
							item.setCasePackaging(item.getMultiplier() + " " + item.getBillUom() + "/" + item.getVendorUom());																					
						}
						if(item.getCurrentCost().equalsIgnoreCase("") || item.getCurrentCost().equalsIgnoreCase("0"))
							item.setCurrentCost(getMedlinePrice(customer, item.getItem()));												
					}	
														
					stmt = conn.prepareStatement("INSERT INTO PARSCAN_ITEM (ID, CUSTOMER, ITEM_ID, VENDOR_ITEM, DESCRIPTION, VENDOR_ID, CATEGORY_ID, VENDOR_UOM, MULTIPLIER, CASE_PACKAGING, CURRENT_COST, FUTURE_COST, FUTURE_COST_DATE, CURRENT_PRICE, FUTURE_PRICE, FUTURE_PRICE_DATE, ALTERNATE_BARCODE, MEDLINE_ITEM, BILL_UOM, PARSCAN_USER, PRICE_CHANGE_DATE, VENDOR_PREFERENCE, COST_CHANGE_DATE, KIT_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				
					if(!item.getFutureCostEffectiveDate().equalsIgnoreCase(""))
						sqlFutureCostDate = new Timestamp(dateFormat.parse(item.getFutureCostEffectiveDate()).getTime());
					if(!item.getFuturePriceEffectiveDate().equalsIgnoreCase(""))
						sqlFuturePriceDate = new Timestamp(dateFormat.parse(item.getFuturePriceEffectiveDate()).getTime());
					if(!item.getCurrentPrice().equalsIgnoreCase(""))				
						sqlPriceChangeDate = new Timestamp(new java.util.Date().getTime());
					if(!item.getCurrentCost().equalsIgnoreCase(""))
						sqlCostChangeDate = new Timestamp(new java.util.Date().getTime());					
				
					UID uid = new UID();
					item.setItemGUID(uid.toString());
					
					stmt.setString(1, item.getItemGUID());
					stmt.setString(2, customer);
					stmt.setString(3, item.getItemID().toUpperCase());
					stmt.setString(4, emptyString(item.getItem().toUpperCase()));
					stmt.setString(5, item.getDescription());
					stmt.setString(6, emptyString(item.getVendor()));
					stmt.setString(7, emptyString(item.getCategory()));
					stmt.setString(8, emptyString(item.getVendorUom()));
					stmt.setString(9, emptyString(item.getMultiplier()));
					stmt.setString(10, emptyString(item.getCasePackaging()));
					stmt.setString(11, emptyString(item.getCurrentCost()));
					stmt.setString(12, emptyString(item.getFutureCost()));
					stmt.setTimestamp(13, sqlFutureCostDate);
					stmt.setString(14, emptyString(item.getCurrentPrice()));
					stmt.setString(15, emptyString(item.getFuturePrice()));				
					stmt.setTimestamp(16, sqlFuturePriceDate);
					stmt.setString(17, emptyString(item.getAlternateBarcode()));
					stmt.setString(18, emptyString(item.getMedlineItem()));
					stmt.setString(19, emptyString(item.getBillUom()));
					stmt.setString(20, user);
					stmt.setTimestamp(21, sqlPriceChangeDate);
					stmt.setInt(22, 1);
					stmt.setTimestamp(23, sqlCostChangeDate);
					stmt.setString(24, null);
					
					logger.debugT(item.getItemID() +" does not exist in db, adding it.");
					
					stmt.executeUpdate();
					stmt.close();	
				}else{
					stmt.close();
					
					if(updateItem.equalsIgnoreCase("X")){
						if(!item.getVendor().equalsIgnoreCase("")){											
							if(item.getVendor().toUpperCase().indexOf("MEDLINE") != -1){
								stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
								stmtVendor.setString(1, customer);
								stmtVendor.setString(2, "Medline Industries");
								ResultSet rsVendor = stmtVendor.executeQuery();
								rsVendor.next();
								//ParScanResidentsSSBean.logger.debugT("medline vendor guid: " + rsVendor.getString("ID"));
								item.setVendor(rsVendor.getString("ID"));
								//ParScanResidentsSSBean.logger.debugT("medline vendor guid: " + item.getVendor());
								stmtVendor.close();		
								if(productValidation(item.getItem())){
									item.setMedlineItem("X");
								}
							}else{		
								//ParScanResidentsSSBean.logger.debugT("Medline not found");					
								boolean foundVendor = false;
							
								stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME <> 'Medline Industries'");
								stmtVendor.setString(1, customer);
								ResultSet rsVendor = stmtVendor.executeQuery();
								while(rsVendor.next()){
									if(rsVendor.getString("VENDOR_NAME").equalsIgnoreCase(item.getVendor())){
										foundVendor = true;
										item.setVendor(rsVendor.getString("ID"));						
										break;
									}
								}				
								stmtVendor.close();	
							
								if(!foundVendor){
									ParScanVendorBean vendorBean = new ParScanVendorBean();						
									vendorBean.setVendorName(item.getVendor());		
									vendorBean.setVendorID(item.getVendor());	
									vendorBean.setNotes("");		
									vendorBean.setAddress("");
									vendorBean.setCity("");
									vendorBean.setFax("");
									vendorBean.setPhone("");
									vendorBean.setState("");
									vendorBean.setZipcode("");											
									addVendor(customer, vendorBean);
						
									stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
									stmtVendor.setString(1, customer);
									stmtVendor.setString(2, item.getVendor());
									rsVendor = stmtVendor.executeQuery();
									rsVendor.next();
									item.setVendor(rsVendor.getString("ID"));
									stmtVendor.close();						
								}
							}						
																
							stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ? AND VENDOR_ID = ?");
							stmt.setString(1, customer);
							stmt.setString(2, item.getItemID().toUpperCase());
							stmt.setString(3, item.getVendor());
							rs = stmt.executeQuery();
							rs.next();
						
							if (rs.getInt("CNT") <= 0){
								stmt.close();
								
								logger.debugT(item.getItemID() +" does exist in db for different vendor so adding it for vendor = " + item.getVendor());
								addVendorProduct(customer, false, item);	
							}							
							else{
								stmt.close();
								stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ? AND VENDOR_ID = ?");
								stmt.setString(1, customer);
								stmt.setString(2, item.getItemID().toUpperCase());
								stmt.setString(3, item.getVendor());
								rs = stmt.executeQuery();
								rs.next();		
							
								item.setPriceChangeDate("");
								item.setCostChangeDate("");
							
								String itemGuid = rs.getString("ID");
								item.setItemGUID(itemGuid);
								
								logger.debugT(item.getItemID() +" does exist in db for vendor = " + item.getVendor() +", so updating it");
								
								if(item.getCurrentPrice().equalsIgnoreCase(rs.getString("CURRENT_PRICE"))){
									stmt.close();
									updateProduct(customer, itemGuid, item, false);						
								}else{
									stmt.close();
									updateProduct(customer, itemGuid, item, true);								
								}
							}						
						}						
					}		
				}
			}
			
			if(!parArea.equalsIgnoreCase("")){
				List stockList = new ArrayList();
				for(Iterator itor = productList.iterator();itor.hasNext();){
					ParScanItemBean item = (ParScanItemBean) itor.next();
					ParScanStockBean stock = new ParScanStockBean();
					if(item.getItemGUID().equalsIgnoreCase("") || item.getItemGUID().equals(null)){
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ? AND VENDOR_PREFERENCE = 1");
						stmt.setString(1, customer);
						stmt.setString(2, item.getItemID());

						ResultSet rs = stmt.executeQuery();
						rs.next();						
						stock.setItemGuid(rs.getString("ID"));
						stmt.close();
					}else
						stock.setItemGuid(item.getItemGUID());

					stock.setItemID(item.getItemID());
					stock.setItemDescription(item.getDescription());
					stock.setOnHandQuantity(item.getOnHandQuantity());
					stock.setParLevel(item.getParLevel());
					stock.setCriticalLevel(item.getCriticalLevel());
					if(venLevelUOM.equalsIgnoreCase(""))
						stock.setCurrentUOM(item.getBillUom());
					else
						stock.setCurrentUOM(item.getVendorUom());
					stock.setVendorGuid(emptyString(item.getVendor()));
					stock.setUpdateAction("Product added to par area");
					stock.setScanned("N");					
					stockList.add(stock);
				}
				addParAreaProducts(customer, parArea, stockList, true);
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
			
			logger.exiting(methodName);
		}
	}	
	
	/**
	 * Business Method.
	 */	
	public void repriceMedlineItems(String customer)throws ParScanResidentsEJBException{
		Connection conn = null;
		
		try{
			conn = openConnection();
			PreparedStatement stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND MEDLINE_ITEM = 'X'");
			stmt.setString(1, customer);
			ResultSet rs = stmt.executeQuery();

			String cost = "";
			PreparedStatement stmtUpdate;
			Timestamp sqlCostChangeDate = new Timestamp(new java.util.Date().getTime());
			while(rs.next()){
				cost = getMedlinePrice(customer, rs.getString("VENDOR_ITEM"));
				
				if(!cost.equalsIgnoreCase("") & cost != null){
					stmtUpdate = conn.prepareStatement("UPDATE PARSCAN_ITEM SET CURRENT_COST = ?, COST_CHANGE_DATE = ? WHERE CUSTOMER = ? AND ID = ?");
					stmtUpdate.setString(1,cost);
					stmtUpdate.setTimestamp(2,sqlCostChangeDate);
					stmtUpdate.setString(3,customer);
					stmtUpdate.setString(4,rs.getString("ID"));
					stmtUpdate.executeUpdate();
					stmtUpdate.close();		
				}
			}
				
			rs.close();			
		}catch(Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during repriceMedlineItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally
		{
			try
			{
				if (conn != null)
				conn.close();
			}
			catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	private int findVendorPreference(String customer, String itemID, String vendorGUID)throws ParScanResidentsEJBException{
		Connection conn = null;
		int curPref = 0;
		
		try{			
			conn = openConnection();
			PreparedStatement stmt =  conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ? AND VENDOR_ID <> ?");
			stmt.setString(1, customer);
			stmt.setString(2, itemID);
			stmt.setString(3, vendorGUID);
			ResultSet rs = stmt.executeQuery();
			
			while(rs.next()){
				if(Integer.parseInt(rs.getString("VENDOR_PREFERENCE")) > curPref)
					curPref = Integer.parseInt(rs.getString("VENDOR_PREFERENCE"));
			}
			
			return curPref + 1;
		}			
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during findVendorPreference: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally
		{
			try
			{
				if (conn != null)
				conn.close();
			}
			catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}
	
	public boolean productValidation(String product)throws ParScanResidentsEJBException{
		IConnection connection = null;
		String returnProduct;
		boolean validMedlineProduct = false;
		
		try{
			connection = getR3Connection();
			
			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "Z_RFC_MARA_ARRAY_READ");
		
			IFunctionsMetaData functionsMetaData = connection.getFunctionsMetaData();
			IFunction function = functionsMetaData.getFunction("Z_RFC_MARA_ARRAY_READ");
		
			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams = recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory = interaction.retrieveStructureFactory();
				
			IRecordSet productsIn = (IRecordSet) structureFactory.getStructure(function.getParameter("IPRE03").getStructure());			

			productsIn.insertRow();
			productsIn.setString("MATNR", product.toUpperCase());

			importParams.put("IPRE03", productsIn);
	
			MappedRecord exportParams = (MappedRecord) interaction.execute(interactionSpec, importParams);
			IRecordSet medProduct = (IRecordSet) exportParams.get("MARA_TAB");

			while (medProduct.next()) {				
				if(!medProduct.getString("MATNR").equalsIgnoreCase("")){
					validMedlineProduct = true;
				}else{
					validMedlineProduct = false;
				}
			}			

		return validMedlineProduct;
			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during productValidation: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally
		{
			try
			{
				if (connection != null)
					connection.close();
			}
			catch (ResourceException ex)
			{
			}
		}	
	}

	public List getOrderTemplates( String customerNumber ) throws ParScanResidentsEJBException
	{
		String methodName = "getOrderTemplates()";
		logger.entering(methodName);				

		List templateList = new ArrayList();
					
		try
		{					
			InitialContext ctx = new InitialContext();			
			
			GetOrderTemplatesServiceService obj = (GetOrderTemplatesServiceService) ctx.lookup("java:comp/env/OrderTemplateProxy");				
			GetOrderTemplatesService port = (GetOrderTemplatesService) obj.getLogicalPort();
			
			String userId = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toString();
							  
			GetOrderTemplatesRequest request = new GetOrderTemplatesRequest();
			request.setUserId(userId);
			request.setAccount(customerNumber);							
			
			GetOrderTemplatesResponse response = (GetOrderTemplatesResponse) port.getOrderTemplatesForUser(request);			
			OrderTemplate[] templates = response.getOrderTemplates();
			
			for( int i=0; i<templates.length; i++ )
			{
				OrderTemplateCategory[] categories = templates[i].getCategories();
				String templateName = templates[i].getTemplateName();
				
				if(categories != null)
				{	
					for( int j=0; j<categories.length; j++ )
					{
						TemplateItem[] items = categories[j].getItems();
						
						for( int k=0; k<items.length; k++ )
						{
							String productName = items[k].getProductName() != null ? items[k].getProductName() : "";
							String itemID = getCMIR(productName, customerNumber);
							String productDescription = items[k].getProductDescription() != null ? items[k].getProductDescription() : "";
							String uom = items[k].getUnitOfMeasure() != null ? items[k].getUnitOfMeasure() : "";																				
							
							ParScanItemBean data = new ParScanItemBean();
							
							data.setOrderTemplate(templateName);
							data.setItem(productName);
							if(itemID.equalsIgnoreCase(""))
								itemID = data.getItem();
							data.setItemID(itemID);
							data.setDescription(productDescription);
							data.setBillUom(uom);

							templateList.add(data);
						}
					}	
				}		
			}									
		}		
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getOrderTemplates: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally
		{
			logger.exiting(methodName);
		}	
			
		return templateList;			
	}

	/*
	 this method parses UOM value returned from web service
	 e.g.
	  for input "1200 EA/CS" method returns EA
	  for input "1/BX" method returns EA
	  for input "PK" method returns PK	
	 */	
	private String[] parseUOMValue( String input )
	{
		String[] parsedValues = new String[3];
		
		String multiplier = null;
		String UOM = null;
		String vendor_uom = null;
		
		input = input.trim();
		
		int slash_index = input.lastIndexOf("/");
		int space_index = input.indexOf(" ");

		if ( space_index > -1 && slash_index > -1 && slash_index > space_index )
		{
			String[] arr1 = input.split("/");
			String left = arr1[0];
			vendor_uom = arr1[1];
			String[] arr2 = left.split(" ");
			multiplier = arr2[0];
			UOM = arr2[1];
		}
		else if ( space_index == -1 && slash_index > -1 )
		{
			UOM = "EA";
			String[] arr1 = input.split("/");
			multiplier = arr1[0];
			vendor_uom = arr1[1];
		}
		
		parsedValues[0] = UOM;
		parsedValues[1] = multiplier;
		parsedValues[2] = vendor_uom;
		
		return parsedValues;		
	}
		
	private String getCMIR(String productNumber, String customerNumber) throws ParScanResidentsEJBException
	{
		try{
			com.sapportals.portal.security.usermanagement.IUser user = WPUMFactory.getServiceUserFactory().getServiceUser("cmadmin_service");
			IResourceContext ctx = new ResourceContext(user);
			RID rid = RID.getRID(getPath(productNumber));
			IResource resource = ResourceFactory.getInstance().getResource(rid, ctx);
			
			if (resource != null){
				//get cmir
				IProperty prop = resource.getProperty(PropertyName.getPN("http://sapportals.com/xmlns/cm", "med_cmir"));
				if (prop != null)
				{
					Iterator values = prop.getValues().iterator();
					while (values.hasNext())
					{
						String value = (String) values.next();
						if (value != null && value.startsWith(customerNumber))
						{
							return value.substring(value.indexOf(":") + 1);
						}
					}
				}
			}else{
				rid = RID.getRID(getPath(productNumber).replaceAll(".txt",".zip"));
				resource = ResourceFactory.getInstance().getResource(rid, ctx);
					
				if (resource != null){
					//get short description
					IProperty prop = resource.getProperty(PropertyName.getPN("http://sapportals.com/xmlns/cm", "med_cmir"));
				  
					if (prop != null && prop.getValue() != null)
					{
							return prop.getValueAsString();
					}
				}			
			}
			return "";
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getCMIR: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
	}	
	
	/**
	 * Business Method.
	 */
	public void updateVendorRelationship(String customer, ParScanItemBean item)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
			stmtVendor.setString(1, customer);
			stmtVendor.setString(2, item.getVendor());

			ResultSet rsVendor = stmtVendor.executeQuery();
			rsVendor.next();				
				
			if(rsVendor.getString("VENDOR_NAME").toUpperCase().indexOf("MEDLINE") != -1){
				if(productValidation(item.getItem())){
					item.setMedlineItem("X");
				}else{
					item.setMedlineItem("");
				}
			}

			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET VENDOR_ID = ?, MEDLINE_ITEM =?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, item.getVendor());
			stmt.setString(2, item.getMedlineItem());
			stmt.setString(3, user);
			stmt.setString(4, customer);
			stmt.setString(5, item.getItemGUID());
			stmt.executeUpdate();
			stmt.close();
			stmtVendor.close();
			
			stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET VENDOR_GUID = ? WHERE CUSTOMER = ? AND ITEM_GUID = ?");
			stmt.setString(1, item.getVendor());
			stmt.setString(2, customer);
			stmt.setString(3, item.getItemGUID());
			stmt.executeUpdate();
			stmt.close();
			
			stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET VENDOR_GUID = ? WHERE CUSTOMER = ? AND ITEM_GUID = ?");
			stmt.setString(1, item.getVendor());
			stmt.setString(2, customer);
			stmt.setString(3, item.getItemGUID());
			stmt.executeUpdate();
			stmt.close();
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateVendor: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void markupProducts(String customer, List productList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtResident;
		Connection conn = null;
		Timestamp sqlFuturePriceDate = null;
		Timestamp sqlPriceChangeDate = null;
		Timestamp todayTS = new Timestamp(new java.util.Date().getTime()); 	
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");	

		try {									
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for (Iterator itor = productList.iterator(); itor.hasNext();) {
				ParScanItemBean data = (ParScanItemBean) itor.next();
				if(!data.getFuturePriceEffectiveDate().equalsIgnoreCase(""))
					sqlFuturePriceDate = new Timestamp(dateFormat.parse(data.getFuturePriceEffectiveDate()).getTime());
				else
					sqlFuturePriceDate = null;						
				sqlPriceChangeDate = new Timestamp(dateFormat.parse(data.getPriceChangeDate()).getTime());
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET CURRENT_PRICE = ?, FUTURE_PRICE = ?, FUTURE_PRICE_DATE = ?, PARSCAN_USER = ?, PRICE_CHANGE_DATE = ? WHERE CUSTOMER = ? AND ID = ?");	
				stmt.setString(1,emptyString(data.getCurrentPrice()));
				stmt.setString(2,emptyString(data.getFuturePrice()));
				stmt.setTimestamp(3,sqlFuturePriceDate);
				stmt.setString(4, user);
				stmt.setTimestamp(5,sqlPriceChangeDate);
				stmt.setString(6, customer);
				stmt.setString(7, data.getItemGUID());
	
				stmt.executeUpdate();
				stmt.close();
	
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during markupProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}		

	/**
	 * Business Method.
	 */
	public void assignPayorCode(String customer, String guid, ParScanPayorCodeBean PayorCode)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		Timestamp sqlEffectiveDate = null;
		Timestamp sqlEndDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			java.util.Date date = new java.util.Date();
			
			if(!PayorCode.getEffectiveDate().equalsIgnoreCase(""))
				sqlEffectiveDate = new Timestamp(dateFormat.parse(PayorCode.getEffectiveDate()).getTime());
			else
				sqlEffectiveDate = new Timestamp(date.getTime());
			if(!PayorCode.getEndDate().equalsIgnoreCase(""))
				sqlEndDate = new Timestamp(dateFormat.parse(PayorCode.getEndDate()).getTime());				
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_PAYORINFO WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND PAYOR_CODE = ? AND EFFECTIVE_DATE = ?");
			stmt.setString(1, customer);
			stmt.setString(2, guid);
			stmt.setString(3, PayorCode.getPayorCode());
			stmt.setTimestamp(4, sqlEffectiveDate);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") <= 0) {
				stmt.close();
				stmt = conn.prepareStatement("UPDATE PARSCAN_PAYORINFO SET END_DATE = ? WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND END_DATE IS NULL");
				stmt.setTimestamp(1, sqlEffectiveDate);
				stmt.setString(2, customer);
				stmt.setString(3, guid);
				stmt.executeUpdate();
				stmt.close();
							
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_PAYORINFO (ID, CUSTOMER, RESIDENT_GUID, PAYOR_CODE, EFFECTIVE_DATE, END_DATE, PARSCAN_USER) VALUES (?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();
				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, guid);
				stmt.setString(4, PayorCode.getPayorCode());
				stmt.setTimestamp(5, sqlEffectiveDate);
				stmt.setTimestamp(6, sqlEndDate);
				stmt.setString(7, user);
				stmt.executeUpdate();
				stmt.close();				
			}else{
				stmt.close();
				if(!PayorCode.getEndDate().equalsIgnoreCase("")){
					stmt = conn.prepareStatement("UPDATE PARSCAN_PAYORINFO SET END_DATE = ? WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND END_DATE IS NULL");
					stmt.setTimestamp(1, sqlEndDate);
					stmt.setString(2, customer);
					stmt.setString(3, guid);
					stmt.executeUpdate();
					stmt.close();					
				}				
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during assignPayorCode: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	//Refactoring: ParScanResidentChargeBean holds header level data plus list of detail beans 
	//This method is just to make existing updateServer() webservice call compatible with the refactored code.
	//We should refactor updateServer() client code and updateServer() should directly call addCharge( ParScanResidentChargeBean charge ) method.
	//
	private void addCharge(String customer, String residentGUID, ParScanResidentChargeBean charge)throws ParScanResidentsEJBException
	{
		ParScanResidentChargeDetailBean detail = new ParScanResidentChargeDetailBean();
		
		//just copy details
		detail.setAltBarcode( charge.getAltBarcode() );
		detail.setArCode( charge.getArCode() );
		detail.setCasePackaging( charge.getCasePackaging() );
		detail.setCharge( charge.getCharge() );
		detail.setChargeDate( charge.getChargeDate() );
		detail.setChargeID( charge.getChargeID() );
		detail.setCost( charge.getCost() );
		detail.setDescription( charge.getDescription() );
		detail.setOnHandQuantity( charge.getOnHandQuantity() );
		detail.setPayorType( charge.getPayorType() );
		detail.setPrice( charge.getPrice() );
		detail.setProductCategory( charge.getProductCategory() );
		detail.setQuantity( charge.getQuantity() );
		detail.setTotal( charge.getTotal() );
		detail.setUom( charge.getUom() );
		detail.setVendor( charge.getVendor() );
		detail.setVendorItem( charge.getVendorItem() );
		detail.setVendorUOM( charge.getVendorUOM() );
		
		charge.addDetail( detail );
		
		addCharge( charge );
	}
	
	
	/**
	 * Business Method.
	 */
	public void addCharge( ParScanResidentChargeBean charge )throws ParScanResidentsEJBException
	{
		String methodName = "addCharge()";
		logger.entering( methodName );
		
		PreparedStatement stmt = null;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try 
		{
			assignPrivatePayPayorCode( charge.getCustomer(), charge.getResidentGUID() );
			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			
			ParScanResidentBean resident = getGUIDResident(charge.getCustomer(), charge.getResidentGUID());
			
			conn = openConnection();
			
			Timestamp sqlChargeDate = null;
			Timestamp sqlDismissDate = null;
			Date now = new Date();
			Date stockDate = new Date();
			Timestamp todayStamp = new Timestamp(stockDate.getTime());
			now = dateFormat.parse(dateFormat.format(now));
			
			List chargeDetails = charge.getDetails();
			for(Iterator iter = chargeDetails.iterator(); iter.hasNext(); )
			{
				ParScanResidentChargeDetailBean detail = (ParScanResidentChargeDetailBean) iter.next();
				sqlChargeDate = getChargeDate(detail, now );
				sqlDismissDate = getResidentDismissDate(resident, sqlChargeDate);
				if(residentNotyetDismissed( sqlDismissDate, sqlChargeDate))
				if(residentNotyetDismissed( sqlDismissDate, sqlChargeDate))
				{
					updateChargePrice(charge, detail, conn);
				}
			}

			Timestamp sqlStartDate = getChargeStartDate( charge );
			Timestamp sqlEndDate = getChargeEndDate( charge, resident, sqlDismissDate );
			
			conn.setAutoCommit( false );
			
			insertChargeDetails( charge, conn, sqlStartDate,  sqlEndDate);
				
			if(charge.getServiceFlag().equalsIgnoreCase(""))
			{
				updateChargedItemStock( charge, conn);
				insertChargedItemStockLog(charge, conn);
			}
			
			conn.commit();
			
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addCharge: " + stringWriter.toString());
			try
			{
				conn.rollback();
			}
			catch( SQLException sqle )
			{
				logger.errorT("rollback failed: message = " + sqle.getMessage() );
				throw new ParScanResidentsEJBException( sqle );
			}
		
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}

	private Timestamp getChargeEndDate( ParScanResidentChargeBean charge, ParScanResidentBean resident,	Timestamp sqlDismissDate ) throws ParseException 
	{
		Timestamp sqlEndDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		if(!charge.getEndDate().equalsIgnoreCase(""))
		{
			sqlEndDate = new Timestamp(dateFormat.parse(charge.getEndDate()).getTime());
			if(!resident.getDismissDate().equalsIgnoreCase(""))
			{
				if(sqlEndDate.after(sqlDismissDate))
					sqlEndDate = sqlDismissDate;
			}
		}					
		else
		{
			if(!resident.getDismissDate().equalsIgnoreCase(""))
			{
				sqlEndDate = sqlDismissDate;
			}					
		}
		return sqlEndDate;
	}

	private Timestamp getChargeStartDate( ParScanResidentChargeBean charge )throws ParseException 
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		if(!charge.getStartDate().equalsIgnoreCase(""))
		{
			return new Timestamp(dateFormat.parse(charge.getStartDate()).getTime());
		}
		return null;
	}

	private void updateChargePrice(	ParScanResidentChargeBean charge, ParScanResidentChargeDetailBean detail, Connection conn) throws SQLException 
	{
		String methodName = "updateChargePrice()";
		logger.entering( methodName );
		
		PreparedStatement stmt = null;
		ResultSet rs = null; 
		
		if(detail.getPrice().equalsIgnoreCase(""))
		{
			if(charge.getServiceFlag().equalsIgnoreCase(""))
			{
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, charge.getCustomer());
				stmt.setString(2, detail.getChargeID());
				rs = stmt.executeQuery();
				rs.next();
						
				if(rs.getString("CURRENT_PRICE") == null)
				{
					detail.setPrice("0");
				}
				else
				{
					detail.setPrice(rs.getString("CURRENT_PRICE"));
				}
									
			}
			else
			{
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_SERVICES WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, charge.getCustomer());
				stmt.setString(2, detail.getChargeID());
				rs = stmt.executeQuery();
				rs.next();
				if(rs.getString("PRICE") == null)
				{
					detail.setPrice("0");
				}
				else
				{
					detail.setPrice(rs.getString("PRICE"));
				}
			}
			
			closeDbResources(stmt, rs);

		}
		
		logger.exiting( methodName );
	}

	private Timestamp getResidentDismissDate( ParScanResidentBean resident, Timestamp sqlChargeDate) throws ParseException
	{
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		Timestamp sqlDismissDate = null;
		
		if(resident != null)
		{
			if(!resident.getDismissDate().equalsIgnoreCase(""))
			{
				sqlDismissDate = new Timestamp(dateFormat.parse(resident.getDismissDate()).getTime());
			}
			else
			{
				sqlDismissDate = sqlChargeDate;
			}				
		}
		else
		{
			sqlDismissDate = sqlChargeDate;
		}
		
		return sqlDismissDate;
			
	}

	private Timestamp getChargeDate( ParScanResidentChargeDetailBean detail, Date now ) throws ParseException
	{
		SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy h:mm:ss aa");
		
		Timestamp sqlChargeDate = null;
		if(detail.getChargeDate().equalsIgnoreCase(""))
		{
			sqlChargeDate = new Timestamp(now.getTime());	
		}				
		else
		{
			sqlChargeDate = new Timestamp(dateTimeFormat.parse(detail.getChargeDate()).getTime());			
		}
		
		return sqlChargeDate;
	}
	
	private boolean residentNotyetDismissed( Timestamp sqlDismissDate, Timestamp sqlChargeDate)
	{
		return sqlChargeDate.before(sqlDismissDate) || sqlChargeDate.equals(sqlDismissDate);
	}
	
	private void insertChargeDetails( ParScanResidentChargeBean charge, Connection conn, Timestamp startDate, Timestamp endDate ) throws Exception
	{
		String methodName = "insertChargeDetails()";
		logger.entering( methodName );
		
		
		PreparedStatement stmt = conn.prepareStatement("INSERT INTO PARSCAN_RESIDENT (ID, CUSTOMER, RESIDENT_GUID, CHARGE_ID, SERVICE_FLAG, QUANTITY, PRICE, START_DATE, END_DATE, RECURRING_FLAG, CHARGE_DATE, PARSCAN_USER, FREQUENCY, FREQUENCY_BETWEEN, PAR_AREA) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
		Date now = dateFormat.parse(dateFormat.format(new Date()));
		
		for ( Iterator iter = charge.getDetails().iterator(); iter.hasNext(); )
		{
			ParScanResidentChargeDetailBean detail = (ParScanResidentChargeDetailBean) iter.next();
			Timestamp chargeDate = getChargeDate(detail, now );
			
			stmt.setString(1, new UID().toString());
			stmt.setString(2, charge.getCustomer());
			stmt.setString(3, charge.getResidentGUID());
			stmt.setString(4, detail.getChargeID());
			stmt.setString(5, emptyString(charge.getServiceFlag()));
			stmt.setString(6, detail.getQuantity());
			stmt.setString(7, detail.getPrice());
			stmt.setTimestamp(8, startDate);
			stmt.setTimestamp(9, endDate);
			stmt.setString(10, emptyString(charge.getRecurringFlag()));
			stmt.setTimestamp(11, chargeDate);
			stmt.setString(12, user);
			stmt.setString(13, emptyString(charge.getFrequency()));
			stmt.setInt(14,1);
			stmt.setString(15, emptyString(charge.getParArea()));
			
			stmt.addBatch();
		}
		stmt.executeBatch();
		stmt.close();
		
		logger.exiting( methodName );
	}
	
	private void insertChargedItemStockLog(ParScanResidentChargeBean charge, Connection conn)throws Exception
	{
		String methodName = "insertChargedItemStockLog()";
		logger.entering( methodName );
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		Date now = dateFormat.parse(dateFormat.format(new Date()));
		
		PreparedStatement stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
		
		for ( Iterator iter = charge.getDetails().iterator(); iter.hasNext(); )
		{
			ParScanResidentChargeDetailBean detail = (ParScanResidentChargeDetailBean) iter.next();
			
			ItemBillUomBean uomBean = getChargedItemBillUom(charge, detail, conn);
			Timestamp chargeDate = getChargeDate(detail, now );
			
			stmt.setString(1, new UID().toString());
			stmt.setString(2, charge.getCustomer());
			stmt.setString(3,detail.getChargeID());
			stmt.setString(4,charge.getParArea());
			stmt.setString(5,detail.getQuantity());
			stmt.setString(6,uomBean.billUOM);
			stmt.setString(7, "Charged to Resident");	
			stmt.setTimestamp(8, chargeDate);	
			stmt.setString(9, "-");	
			
			stmt.addBatch();
		}
		stmt.executeBatch();
		stmt.close();
		
		logger.exiting( methodName );

	}
		
	
	private void updateChargedItemStock( ParScanResidentChargeBean charge, Connection conn) throws Exception
	{
		String methodName = "updateChargedItemStock()";
		logger.entering( methodName );
		
		
		String stockUOM = "";
		String newHandQty = "";
		String handQty = "";
		String criticalLevel = "";
		String parLevel = "";
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			
		PreparedStatement stmtStock = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
		
		PreparedStatement stmt1 = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_ACTION = ?, CHANGE_TIMESTAMP = ? WHERE CUSTOMER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
		
		PreparedStatement stmt2 = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CRITICAL_LEVEL = ?, PAR_LEVEL = ?, UOM = ?, CHANGE_ACTION = ?, CHANGE_TIMESTAMP = ? WHERE CUSTOMER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
		
		for( Iterator iter = charge.getDetails().iterator(); iter.hasNext(); )
		{
			ParScanResidentChargeDetailBean detail = (ParScanResidentChargeDetailBean) iter.next();
			//Get stock UOM
			stmtStock.setString(1, charge.getCustomer());
			stmtStock.setString(2, detail.getChargeID());
			stmtStock.setString(3, charge.getParArea());
			ResultSet rsStock = stmtStock.executeQuery();
			rsStock.next();
			stockUOM = rsStock.getString("UOM");
			criticalLevel = rsStock.getString("CRITICAL_LEVEL");
			parLevel = rsStock.getString("PAR_LEVEL");
			if(detail.getOnHandQuantity().equalsIgnoreCase(""))
			{
				handQty = rsStock.getString("ON_HAND_QUANTITY");
			}
			else
			{
				handQty = detail.getOnHandQuantity();
			}
			rsStock.close();

			//Get item UOM
			
			ItemBillUomBean uomBean = getChargedItemBillUom(charge, detail, conn);
			
	
			//Check UOM's
			if(stockUOM.equalsIgnoreCase(uomBean.billUOM))
			{
				if(Double.parseDouble(handQty) - Double.parseDouble(detail.getQuantity()) >= 0)
				{
					newHandQty = Integer.toString((int)Math.ceil(Double.parseDouble(handQty) - Double.parseDouble(detail.getQuantity())));
				}					
				else
				{
					newHandQty = "0";
				}
						
				//update stock table
				stmt1.setString(1, newHandQty);
				stmt1.setString(2, "Stock Change: Resident Charge");					
				stmt1.setTimestamp(3, getChargeDate( detail, dateFormat.parse( dateFormat.format( new Date() ) ) ) );
				stmt1.setString(4, charge.getCustomer());
				stmt1.setString(5, detail.getChargeID());
				stmt1.setString(6, charge.getParArea());
				stmt1.executeUpdate();
			}
			else
			{
				//Convert stock to bill UOM															
				if(Double.parseDouble(handQty) * uomBean.multiplier - Double.parseDouble(detail.getQuantity()) >= 0)
				{
					newHandQty = Integer.toString((int)Math.ceil(Double.parseDouble(handQty) * uomBean.multiplier - Double.parseDouble(detail.getQuantity())));
				}
				else
				{
					newHandQty = "0";
				}

				criticalLevel = Integer.toString((int)Math.ceil(Double.parseDouble(criticalLevel)* uomBean.multiplier));
				parLevel = Integer.toString((int)Math.ceil(Double.parseDouble(parLevel) * uomBean.multiplier));
		
				//update stock table
				stmt2.setString(1, newHandQty);
				stmt2.setString(2, criticalLevel);
				stmt2.setString(3, parLevel);
				stmt2.setString(4, uomBean.billUOM);
				stmt2.setString(5, "Stock Change: Resident Charge");					
				stmt2.setTimestamp(6, getChargeDate( detail, dateFormat.parse( dateFormat.format( new Date() ) ) ) );					
				stmt2.setString(7, charge.getCustomer());
				stmt2.setString(8, detail.getChargeID());
				stmt2.setString(9, charge.getParArea());
				stmt2.executeUpdate();
			}
		}
		stmtStock.close();
		stmt1.close();
		stmt2.close();
		
		logger.exiting( methodName );
						
	}
	
	private ItemBillUomBean getChargedItemBillUom(ParScanResidentChargeBean charge, ParScanResidentChargeDetailBean detail, Connection conn) throws SQLException
	{
		String methodName = "getChargedItemBillUom()";
		logger.entering(methodName);
		
		PreparedStatement stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
		
		stmt.setString(1, charge.getCustomer());
		stmt.setString(2, detail.getChargeID());
		
		ResultSet rs = stmt.executeQuery();
		rs.next();
		
		ItemBillUomBean uomBean = new ItemBillUomBean();
		uomBean.billUOM = rs.getString("BILL_UOM");
		uomBean.multiplier = StringUtils.isNotEmpty(rs.getString("MULTIPLIER")) ? Double.parseDouble(rs.getString("MULTIPLIER")) : 1;
				
		rs.close();
		stmt.close();
		
		return uomBean;

	}
	
	private void assignPrivatePayPayorCode(String customer, String residentGuid)throws ParScanResidentsEJBException 
	{
		String methodName = "assignPrivatePayPayorCode()";
		logger.entering( methodName );
		
		PreparedStatement stmt = null;
		Connection conn = null;
		Timestamp sqlEffectiveDate = new Timestamp(new Date().getTime());
		Timestamp sqlEndDate = null;

		try 
		{				
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_PAYORINFO WHERE CUSTOMER = ? AND RESIDENT_GUID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, residentGuid);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") <= 0) 
			{
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_PAYORINFO (ID, CUSTOMER, RESIDENT_GUID, PAYOR_CODE, EFFECTIVE_DATE, END_DATE, PARSCAN_USER) VALUES (?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();
	
				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3, residentGuid);
				stmt.setString(4, "N/A");
				stmt.setTimestamp(5, sqlEffectiveDate);
				stmt.setTimestamp(6, sqlEndDate);
				stmt.setString(7, user);
				stmt.executeUpdate();
			
			}
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during assignPrivatePayPayorCode: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			closeDbResources( conn, stmt );
			logger.exiting( methodName );
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void removeCharges(String customer, String resident, List chargeList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			java.util.Date stockDate = new java.util.Date();
			Timestamp todayStamp = new Timestamp(stockDate.getTime());			
			conn = openConnection();
			
			for (Iterator itor = chargeList.iterator(); itor.hasNext();) {
				ParScanResidentChargeBean data = (ParScanResidentChargeBean) itor.next();
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, resident);
				stmt.setString(3, data.getChargeID());
				ResultSet rs = stmt.executeQuery();
				
				if(rs.next()){
					if(rs.getString("SERVICE_FLAG") == null){
						String itemGUID = rs.getString("CHARGE_ID");
						String parArea = rs.getString("PAR_AREA");
						stmt.close();
						
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ?  AND ITEM_GUID = ?");
						stmt.setString(1, customer);
						stmt.setString(2, parArea);
						stmt.setString(3, itemGUID);
						rs = stmt.executeQuery();
						if(rs.next()){
							String uom = rs.getString("UOM");
							double handQty = Double.parseDouble(rs.getString("ON_HAND_QUANTITY"));
							double reorder = Double.parseDouble(rs.getString("CRITICAL_LEVEL"));
							double par = Double.parseDouble(rs.getString("PAR_LEVEL"));
							stmt.close();
							
							if(data.getUom().equals(uom)){
								handQty = handQty + Double.parseDouble(data.getQuantity());
								stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_ACTION = ?, CHANGE_TIMESTAMP = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
								stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
								stmt.setString(2, "Stock Change: Returned from Resident");					
								stmt.setTimestamp(3, todayStamp);								
								stmt.setString(4, customer);
								stmt.setString(5, parArea);
								stmt.setString(6, itemGUID);
								stmt.executeUpdate();
								stmt.close();
								
								UID uid = new UID();			
								stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
								stmt.setString(1, uid.toString());
								stmt.setString(2, customer);
								stmt.setString(3,itemGUID);
								stmt.setString(4,parArea);
								stmt.setString(5,data.getQuantity());
								stmt.setString(6,data.getUom());
								stmt.setString(7, "Returned from Resident");	
								stmt.setTimestamp(8, todayStamp);	
								stmt.setString(9, "+");	
								stmt.executeUpdate();								
							}else{
								stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
								stmt.setString(1, customer);
								stmt.setString(2, itemGUID);
								rs = stmt.executeQuery();
								
								if(rs.next()){
									double multiplier = Double.parseDouble(rs.getString("MULTIPLIER"));
									handQty = handQty*multiplier + Double.parseDouble(data.getQuantity());
									par = par*multiplier;
									reorder = reorder*multiplier;
									stmt.close();
									
									stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, PAR_LEVEL = ?, CRITICAL_LEVEL = ?, CHANGE_ACTION = ?, CHANGE_TIMESTAMP = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
									stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
									stmt.setString(2, Integer.toString((int)Math.ceil(par)));
									stmt.setString(3, Integer.toString((int)Math.ceil(reorder)));
									stmt.setString(4, "Stock Change: Returned from Resident");					
									stmt.setTimestamp(5, todayStamp);									
									stmt.setString(6, customer);
									stmt.setString(7, parArea);
									stmt.setString(8, itemGUID);
									stmt.executeUpdate();
									stmt.close();
								
									UID uid = new UID();			
									stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
									stmt.setString(1, uid.toString());
									stmt.setString(2, customer);
									stmt.setString(3,itemGUID);
									stmt.setString(4,parArea);
									stmt.setString(5,data.getQuantity());
									stmt.setString(6,data.getUom());
									stmt.setString(7, "Returned from Resident");	
									stmt.setTimestamp(8, todayStamp);	
									stmt.setString(9, "+");	
									stmt.executeUpdate();										
								}
							}
						}						
					}			
				}
				
				stmt.close();
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, resident);
				stmt.setString(3, data.getChargeID());
				stmt.executeUpdate();
				stmt.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeCharges: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */	
	public void updateVendorPreference(String customer, ParScanItemBean newItem, ParScanItemBean oldItem)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET VENDOR_PREFERENCE =?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ID = ?");
			stmt.setInt(1, Integer.parseInt(newItem.getVendorPreference()));
			stmt.setString(2, user);
			stmt.setString(3, customer);
			stmt.setString(4, newItem.getItemGUID());
			stmt.executeUpdate();
			stmt.close();
			
			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET VENDOR_PREFERENCE =?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND ID = ?");
			stmt.setInt(1, Integer.parseInt(oldItem.getVendorPreference()));
			stmt.setString(2, user);
			stmt.setString(3, customer);
			stmt.setString(4, oldItem.getItemGUID());
			stmt.executeUpdate();
			stmt.close();			
			
			//Update stock table if new preferred vendor
			if(newItem.getVendorPreference().equalsIgnoreCase("1")){
				updateStockPreference(customer, newItem, oldItem);			
			}else if(oldItem.getVendorPreference().equalsIgnoreCase("1")){
				updateStockPreference(customer, oldItem, newItem);
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateVendorPreference: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}
	
	private void updateStockPreference(String customer, ParScanItemBean newItem, ParScanItemBean oldItem)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
			stmtItem.setString(1, customer);
			stmtItem.setString(2, newItem.getItemGUID());
			ResultSet rs = stmtItem.executeQuery();
			rs.next();

			stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ITEM_GUID =?, PARSCAN_USER= ?, VENDOR_GUID = ? WHERE CUSTOMER = ? AND ITEM_GUID = ?");
			stmt.setString(1, newItem.getItemGUID());
			stmt.setString(2, user);
			stmt.setString(3, rs.getString("VENDOR_ID"));
			stmt.setString(4, customer);
			stmt.setString(5, oldItem.getItemGUID());
			stmt.executeUpdate();
			stmt.close();
			stmtItem.close();
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateStockPreference: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	/**
	 * Business Method.
	 */	
	public void updateVendorProduct(String customer, String itemID, ParScanItemBean item)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;
		Timestamp sqlFutureCostDate = null;
		Timestamp sqlCostChangeDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");		

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
			stmtItem.setString(1, customer);
			stmtItem.setString(2, itemID);
			ResultSet rs = stmtItem.executeQuery();
			rs.next();
			
			if(!item.getFutureCostEffectiveDate().equalsIgnoreCase(""))
				sqlFutureCostDate = new Timestamp(dateFormat.parse(item.getFuturePriceEffectiveDate()).getTime());
			if(!item.getCostChangeDate().equalsIgnoreCase(""))				
				sqlCostChangeDate = new Timestamp(new java.util.Date().getTime());

			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET VENDOR_ITEM =?, VENDOR_UOM = ?, MULTIPLIER = ?, CASE_PACKAGING = ?, CURRENT_COST = ?, FUTURE_COST = ?, FUTURE_COST_DATE = ?, PARSCAN_USER= ?, COST_CHANGE_DATE = ? WHERE CUSTOMER = ? AND ID = ?");

			stmt.setString(1, emptyString(item.getItem()));
			stmt.setString(2, emptyString(item.getVendorUom()));
			stmt.setString(3, emptyString(item.getMultiplier()));
			stmt.setString(4, emptyString(item.getMultiplier()) + " " + rs.getString("BILL_UOM") + "/" + emptyString(item.getVendorUom()));
			stmt.setString(5, emptyString(item.getCurrentCost()));
			stmt.setString(6, emptyString(item.getFutureCost()));
			stmt.setTimestamp(7, sqlFutureCostDate);
			stmt.setString(8, user);
			stmt.setTimestamp(9, sqlCostChangeDate);
			stmt.setString(10, customer);
			stmt.setString(11, item.getItemGUID());

			stmt.executeUpdate();
			stmt.close();
			stmtItem.close();
			
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateVendorProduct: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}
	
	private void updateStockUOM(String customer, String itemGUID, ParScanItemBean item)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtUpdate;
		Connection conn = null;
		
		try{
			conn = openConnection();
			
			stmt = conn.prepareStatement("select * from parscan_item where CUSTOMER = ? and ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, item.getItemGUID());
			ResultSet rs = stmt.executeQuery();
			rs.next();
			
			String itemIUM = rs.getString("BILL_UOM");
			String itemPUM = rs.getString("VENDOR_UOM");
			stmt.close();
			
			stmt = conn.prepareStatement("select * from parscan_stock where customer = ? and item_guid = ?");
			stmt.setString(1, customer);
			stmt.setString(2, item.getItemGUID());
			rs = stmt.executeQuery();
			
			String parUM = "";
			while(rs.next()){
				parUM = rs.getString("UOM");
				
				if(parUM.equalsIgnoreCase(itemIUM)){
					stmtUpdate = conn.prepareStatement("update parscan_stock set UOM = ? where customer = ? and item_guid = ? and par_area = ?");
					stmtUpdate.setString(1,item.getBillUom());
					stmtUpdate.setString(2,customer);
					stmtUpdate.setString(3,item.getItemGUID());
					stmtUpdate.setString(4,rs.getString("PAR_AREA"));
					stmtUpdate.executeUpdate();
					stmtUpdate.close();					
				}else if(parUM.equalsIgnoreCase(itemPUM)){
					stmtUpdate = conn.prepareStatement("update parscan_stock set UOM = ? where customer = ? and item_guid = ? and par_area = ?");
					stmtUpdate.setString(1,item.getVendorUom());
					stmtUpdate.setString(2,customer);
					stmtUpdate.setString(3,item.getItemGUID());
					stmtUpdate.setString(4,rs.getString("PAR_AREA"));
					stmtUpdate.executeUpdate();
					stmtUpdate.close();				
				}
			}
			stmt.close();
		}catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateStockUOM: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
	
	/**
	 * Business Method.
	 */
	public void updateProduct(String customer, String itemGUID, ParScanItemBean item, boolean priceChange)
		throws ParScanResidentsEJBException {
			
		String methodName = "updateProduct()";
		logger.entering(methodName);
		
		PreparedStatement stmt;
		PreparedStatement stmtResident;
		Connection conn = null;
		Timestamp sqlFutureCostDate = null;
		Timestamp sqlFuturePriceDate = null;
		Timestamp sqlPriceChangeDate = null;
		Timestamp sqlCostChangeDate = null;	
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");	

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			updateStockUOM(customer, itemGUID, item);

			DateTime yesterday = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())));
			yesterday = yesterday.minus(1);			
			Timestamp yesterdayDate = new Timestamp(new java.util.Date(yesterday.getMillis()).getTime());

			if(!item.getFutureCostEffectiveDate().equalsIgnoreCase(""))
				sqlFutureCostDate = new Timestamp(dateFormat.parse(item.getFutureCostEffectiveDate()).getTime());
			if(!item.getFuturePriceEffectiveDate().equalsIgnoreCase(""))
				sqlFuturePriceDate = new Timestamp(dateFormat.parse(item.getFuturePriceEffectiveDate()).getTime());
			if(!item.getPriceChangeDate().equalsIgnoreCase(""))				
				sqlPriceChangeDate = new Timestamp(dateFormat.parse(item.getPriceChangeDate()).getTime());
			if(!item.getCostChangeDate().equalsIgnoreCase(""))				
				sqlCostChangeDate = new Timestamp(dateFormat.parse(item.getCostChangeDate()).getTime());				

			if(item.getVendor().toUpperCase().indexOf("MEDLINE") != -1 || item.getMedlineItem().equalsIgnoreCase("X")){
				if(productValidation(item.getItem())){
					item.setMedlineItem("X");
					if(Double.parseDouble(item.getCurrentCost()) <= 0){
						item.setCurrentCost(getMedlinePrice(customer, item.getItem()));	
						sqlCostChangeDate = new Timestamp(new java.util.Date().getTime());
					}						
				}else{
					item.setMedlineItem("");
				}
			}				

			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET ITEM_ID = ?, DESCRIPTION = ?, CURRENT_PRICE = ?, FUTURE_PRICE = ?, FUTURE_PRICE_DATE = ?, ALTERNATE_BARCODE = ?, MEDLINE_ITEM = ? , BILL_UOM = ?, PARSCAN_USER= ?, PRICE_CHANGE_DATE = ? WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, emptyString(item.getItemID()));			
			stmt.setString(2, emptyString(item.getDescription()));
			stmt.setString(3, emptyString(item.getCurrentPrice()));
			stmt.setString(4, emptyString(item.getFuturePrice()));
			stmt.setTimestamp(5, sqlFuturePriceDate);
			stmt.setString(6, emptyString(item.getAlternateBarcode()));
			stmt.setString(7, emptyString(item.getMedlineItem()));
			stmt.setString(8, emptyString(item.getBillUom()));
			stmt.setString(9, user);
			stmt.setTimestamp(10, sqlPriceChangeDate);
			stmt.setString(11, customer);
			stmt.setString(12, item.getItemGUID());
			stmt.executeUpdate();
			stmt.close();
			
			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET VENDOR_ITEM = ?, VENDOR_UOM = ?, MULTIPLIER = ?, CASE_PACKAGING = ?, CURRENT_COST = ?, FUTURE_COST = ?, FUTURE_COST_DATE = ?, PARSCAN_USER= ?, COST_CHANGE_DATE = ? WHERE CUSTOMER = ? AND ID = ?");			
			stmt.setString(1, emptyString(item.getItem()));
			stmt.setString(2, emptyString(item.getVendorUom()));
			stmt.setString(3, emptyString(item.getMultiplier()));
			stmt.setString(4, emptyString(item.getCasePackaging()));
			stmt.setString(5, emptyString(item.getCurrentCost()));
			stmt.setString(6, emptyString(item.getFutureCost()));
			stmt.setTimestamp(7, sqlFutureCostDate);
			stmt.setString(8, user);
			stmt.setTimestamp(9, sqlCostChangeDate);
			stmt.setString(10, customer);
			stmt.setString(11, item.getItemGUID());
			stmt.executeUpdate();
			stmt.close();			
			
			stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ITEM_ID = ? WHERE CUSTOMER = ? AND ITEM_GUID = ?");			
			stmt.setString(1, emptyString(item.getItemID()));
			stmt.setString(2, customer);
			stmt.setString(3, item.getItemGUID());
			stmt.executeUpdate();
			stmt.close();					
			
			stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET ITEM_ID = ? WHERE CUSTOMER = ? AND ITEM_GUID = ?");			
			stmt.setString(1, emptyString(item.getItemID()));
			stmt.setString(2, customer);
			stmt.setString(3, item.getItemGUID());
			stmt.executeUpdate();
			stmt.close();								
			
			if(StringUtils.isNotEmpty(item.getCategory())){
				boolean foundCategory = false;
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?");
				stmt.setString(1, customer);
				ResultSet rsCategory = stmt.executeQuery();
				while(rsCategory.next()){
					if(rsCategory.getString("PRODUCT_CATEGORY").equalsIgnoreCase(item.getCategory())){
						foundCategory = true;
						item.setCategory(rsCategory.getString("ID"));						
						break;
					}
				}				
				stmt.close();
				
				if(!foundCategory){
					ParScanCategoryBean categoryBean = new ParScanCategoryBean();						
					categoryBean.setARCode("");
					categoryBean.setExcludeFlag("");
					categoryBean.setOverrideFlag("");
					categoryBean.setProductCategory(item.getCategory());						
					addProductCategory(customer, categoryBean);
						
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND PRODUCT_CATEGORY = ?");
					stmt.setString(1, customer);
					stmt.setString(2, item.getCategory());
					rsCategory = stmt.executeQuery();
					rsCategory.next();
					item.setCategory(rsCategory.getString("ID"));
					stmt.close();												
				}				
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET CATEGORY_ID = ? WHERE CUSTOMER = ? AND ID = ?");			
				stmt.setString(1, item.getCategory());
				stmt.setString(2, customer);
				stmt.setString(3, item.getItemGUID());
				stmt.executeUpdate();
				stmt.close();
			}			
			
			if(priceChange)
			{
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CHARGE_ID = ? AND CUSTOMER = ? AND RECURRING_FLAG = 'X'");
				stmt.setString(1, item.getItemGUID());
				stmt.setString(2, customer);
				
				ResultSet rs = stmt.executeQuery();
				
				while(rs.next())
				{
					DateTime today = new DateTime(new java.util.Date().getTime());
					
					DateTime startDate = null;
					DateTime endDate = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())).getTime());
					DateTime chargeDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("CHARGE_DATE"))).getTime());
					
					if(rs.getTimestamp("START_DATE") != null)
					{
						startDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("START_DATE"))).getTime());
					}
					else
					{		
						ParScanResidentsSSBean.logger.errorT(" START_DATE from PARSCAN_RESIDENT table returned null ");			
						startDate = (chargeDate.isBefore(endDate) || chargeDate.equals(endDate)) ? chargeDate : endDate ;
					}
					
					if(startDate.isAfter(today))
					{						
						stmtResident = conn.prepareStatement("UPDATE PARSCAN_RESIDENT PRICE = ? WHERE CHARGE_ID = ? AND CUSTOMER = ? AND RECURRING_FLAG = 'X'");
						stmtResident.setString(1, item.getCurrentPrice());
						stmt.setString(2, item.getItemGUID());
						stmt.setString(3, customer);
						stmtResident.executeUpdate();
						stmtResident.close();	
					}
					else if(endDate.isAfter(today) || endDate.equals(today))
					{						
						stmtResident = conn.prepareStatement("UPDATE PARSCAN_RESIDENTS SET END_DATE = ? WHERE CUSTOMER = ? AND ID = ?");
						stmtResident.setTimestamp(1, yesterdayDate);
						stmtResident.setString(2, customer);
						stmtResident.setString(3, rs.getString("ID"));
						stmtResident.executeUpdate();
						stmtResident.close();
											
						stmtResident = conn.prepareStatement("INSERT INTO PARSCAN_RESIDENT (ID, CUSTOMER, RESIDENT_GUID, CHARGE_ID, SERVICE_FLAG, QUANTITY, PRICE, START_DATE, END_DATE, RECURRING_FLAG, CHARGE_DATE, PARSCAN_USER, FREQUENCY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");						
						UID uid = new UID();
						stmtResident.setString(1, uid.toString());
						stmtResident.setString(2, customer);
						stmtResident.setString(3, rs.getString("RESIDENT_GUID"));
						stmtResident.setString(4, item.getItemGUID());
						stmtResident.setString(5, rs.getString("SERVICE_FLAG"));
						stmtResident.setString(6, rs.getString("QUANTITY"));
						stmtResident.setString(7, item.getCurrentPrice());
						stmtResident.setTimestamp(8, new Timestamp(startDate.getMillis()));
						stmtResident.setTimestamp(9, rs.getTimestamp("END_DATE"));
						stmtResident.setString(10, rs.getString("RECURRING_FLAG"));
						stmtResident.setTimestamp(11, sqlPriceChangeDate);
						stmtResident.setString(12, user);
						stmtResident.setString(13, rs.getString("FREQUENCY"));
						stmtResident.executeUpdate();
						stmtResident.close();	
					}									
				}
				stmt.close();
			}			
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateProduct: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			try 
			{
				if (conn != null)
					conn.close();
			} 
			catch (SQLException e) 
			{
				
				e.printStackTrace();
			}
			
			logger.exiting(methodName);
		}
	}
	
	/**
	 * Business Method.
	 */
	public void addUOM(String customer, ParScanUOMBean uom)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		InitialContext initialContext = null;
		Connection conn = null;
		String key = "";

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			initialContext = new InitialContext();
			ApplicationConfigHandlerFactory cfgHandler = (ApplicationConfigHandlerFactory) initialContext.lookup("ApplicationConfiguration");
			Properties appProps = cfgHandler.getApplicationProperties();

			boolean uomExists = appProps.containsKey(uom.getUom());

			if(!uomExists){
				stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_UOM WHERE CUSTOMER = ? AND UOM = ?");
				stmt.setString(1, customer);
				stmt.setString(2, uom.getUom());

				ResultSet rs = stmt.executeQuery();
				rs.next();
				if (rs.getInt("CNT") <= 0) {
					stmt.close();
					stmt = conn.prepareStatement("INSERT INTO PARSCAN_UOM (ID, CUSTOMER, UOM, DESCRIPTION, PARSCAN_USER) VALUES (?, ?, ?, ?, ?)");
					UID uid = new UID();

					stmt.setString(1, uid.toString());
					stmt.setString(2, customer);
					stmt.setString(3, uom.getUom());
					stmt.setString(4, emptyString(uom.getDescription()));
					stmt.setString(5, user);

					stmt.executeUpdate();
					stmt.close();
				}
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during addUOM: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void removeUOM(String customer, List uomList)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			for (Iterator itor = uomList.iterator(); itor.hasNext();) {
				ParScanUOMBean data = (ParScanUOMBean) itor.next();
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_UOM WHERE CUSTOMER = ? AND UOM = ?");

				stmt.setString(1, customer);
				stmt.setString(2, data.getUom());

				stmt.executeUpdate();
				stmt.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removeUOM: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void addUpdateResident(String customer, ParScanResidentBean resident, ParScanPayorCodeBean payor) throws ParScanResidentsEJBException, SimilarResidentExistException 
	{
		String methodName = "addUpdateResident()";
		logger.entering( methodName );

		IConnection connection = null;
		Connection conn = null;
		PreparedStatement stmt;
		List residentList = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		if(!resident.isForceAdd() )
		{
			checkIfSimilarResidentExist( resident );
		}

		try
		{
			conn = openConnection();
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			Date convertedAdmitDate = null;
			Date convertedDismissDate  = null;
			
			if(!resident.getAdmitDate().equalsIgnoreCase(""))
				convertedAdmitDate = dateFormat.parse(resident.getAdmitDate());	
			if(!resident.getDismissDate().equalsIgnoreCase("")){
				convertedDismissDate = dateFormat.parse(resident.getDismissDate());
				Timestamp sqlEndDatesqlEndDate = new Timestamp(convertedDismissDate.getTime());
								
				if(!resident.getCrmGUID().equalsIgnoreCase("")){
					stmt = conn.prepareStatement("UPDATE PARSCAN_RESIDENT SET END_DATE = ? WHERE CUSTOMER = ? AND RECURRING_FLAG = 'X' AND RESIDENT_GUID = ? AND END_DATE IS NULL");				
					stmt.setTimestamp(1, sqlEndDatesqlEndDate);
					stmt.setString(2, customer);
					stmt.setString(3, resident.getCrmGUID());

					stmt.executeUpdate();
					stmt.close();
					
					stmt = conn.prepareStatement("UPDATE PARSCAN_RESIDENT SET END_DATE = ? WHERE CUSTOMER = ? AND RECURRING_FLAG = 'X' AND RESIDENT_GUID = ? AND END_DATE > ?");				
					stmt.setTimestamp(1, sqlEndDatesqlEndDate);
					stmt.setString(2, customer);
					stmt.setString(3, resident.getCrmGUID());
					stmt.setTimestamp(4, sqlEndDatesqlEndDate);

					stmt.executeUpdate();
					stmt.close();					
				}				
			}
				 						
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "Z_PARSCAN_PATIENT_MAINTAIN");

			IFunctionsMetaData functionsMetaData =
				connection.getFunctionsMetaData();
			IFunction function =
				functionsMetaData.getFunction("Z_PARSCAN_PATIENT_MAINTAIN");

			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams =
				recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory =
				interaction.retrieveStructureFactory();

			importParams.put("IN_SOLDTO", customer);
			importParams.put("IN_USER", user);
	
			// Input values
			IRecordSet residentIn = (IRecordSet) structureFactory.getStructure(function.getParameter("T_PATIENT_DATA").getStructure());

			residentIn.insertRow();
			residentIn.setString("GUID", resident.getCrmGUID());
			residentIn.setString("SOLDTO",customer);
			residentIn.setString("PATIENT_ID",resident.getId());			
			residentIn.setString("PFNAME",resident.getFirstName());	
			residentIn.setString("PLNAME", resident.getLastName());
			residentIn.setDate("ADMIT_DATE",convertedAdmitDate);
			residentIn.setDate("DISMISS_DATE", convertedDismissDate);
			residentIn.setString("ROOM_NUMBER",resident.getRoom());
			residentIn.setString("FLOOR", resident.getFloor());
			residentIn.setString("WING",resident.getWing());
			residentIn.setString("CARE_TYPE", resident.getCategory());												

			importParams.put("T_PATIENT_DATA", residentIn);
			
			MappedRecord exportParams = (MappedRecord) interaction.execute(interactionSpec, importParams);
			IRecordSet message = (IRecordSet) exportParams.get("MESSAGELINE");
			while (message.next()) {
				ParScanResidentsSSBean.logger.debugT("Return: " + message.getString("MESSAGE"));
			}
			
			if(!payor.getPayorCode().equalsIgnoreCase("")){
				addPayor(customer,payor);
				
				String lastName = resident.getLastName();
				if(lastName.indexOf(" ") != -1){
					lastName = lastName.substring(0,lastName.indexOf(" "));
				}
				
				ParScanResidentBean data = new ParScanResidentBean();			
				data = getResident(customer, resident.getId(), lastName, "", "", "","","");
				if(data != null)
					assignPayorCode(customer,data.getCrmGUID(), payor);				
			}
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during addUpdateResident: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}		
		finally
		{
			try
			{
				if (connection != null)
					connection.close();
				if (conn != null)
					conn.close();
				
				logger.exiting( methodName );
			}
			catch (ResourceException ex)
			{
				logger.errorT(" Failed to close R3 connection: " + ex.toString());
			} 
			catch (SQLException e) 
			{
				logger.errorT(" Failed to close DB connection: " + e.toString());
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void removeResident(String customer, String residentGUID) throws ParScanResidentsEJBException {
		IConnection connection = null;
		PreparedStatement stmt;
		Connection conn = null;		

		try{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();					
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "Z_PARSCAN_PATIENT_MAINTAIN");

			IFunctionsMetaData functionsMetaData =
				connection.getFunctionsMetaData();
			IFunction function =
				functionsMetaData.getFunction("Z_PARSCAN_PATIENT_MAINTAIN");

			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams =
				recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory =
				interaction.retrieveStructureFactory();

			importParams.put("IN_SOLDTO", customer);
			importParams.put("IN_USER", user);
	
			// Input values
			IRecordSet residentIn = (IRecordSet) structureFactory.getStructure(function.getParameter("T_PATIENT_DATA").getStructure());

			residentIn.insertRow();
			residentIn.setString("GUID", residentGUID);
			residentIn.setString("SOLDTO",customer);
			residentIn.setString("DELETED","X");														

			importParams.put("T_PATIENT_DATA", residentIn);			
			MappedRecord exportParams = (MappedRecord) interaction.execute(interactionSpec, importParams);	
			
			conn = openConnection();
			stmt = conn.prepareStatement("DELETE FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND RESIDENT_GUID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, residentGUID);
			stmt.executeUpdate();
			stmt.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during removeResident: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}		
		finally
		{
			try
			{
				if (connection != null)
					connection.close();
				if (conn != null)
					conn.close();					
			}
			catch (ResourceException ex)
			{
				logger.errorT(" Failed to close R3 connection: " + ex.toString());
			} catch (SQLException e) {
				
				logger.errorT(" Failed to close DB connection: " + e.toString());
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public List getPOList(String customer, String fromDate, String toDate, boolean pendingOnly) throws ParScanResidentsEJBException {
		List poList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtPending;
		PreparedStatement stmtVendor;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();
			Timestamp sqlFromDate = new Timestamp(dateFormat.parse(fromDate).getTime());
			Timestamp sqlToDate = new Timestamp(dateFormat.parse(toDate).getTime());

			stmt = conn.prepareStatement("SELECT DISTINCT PO_NUMBER FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND CREATE_DATE BETWEEN ? AND ?");
			stmt.setString(1, customer);
			stmt.setTimestamp(2, sqlFromDate);
			stmt.setTimestamp(3, sqlToDate);

			ResultSet rs = stmt.executeQuery();
			String vendorGUID = "";
			String vendor = "";
			String vendorNum = "";
			String PO = "";
			String shipTo = "";
			String createDate = "";
			String receivedFlag = "1";
			while (rs.next()) {
				PO = rs.getString("PO_NUMBER");				
				stmtVendor = conn.prepareStatement("SELECT DISTINCT VENDOR_GUID FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, rs.getString("PO_NUMBER"));
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				vendorGUID = rsVendor.getString("VENDOR_GUID");
				stmtVendor.close();
				
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, vendorGUID);
				rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				vendor = rsVendor.getString("VENDOR_NAME");
				vendorNum = rsVendor.getString("VENDOR_ID");
				stmtVendor.close();
				
				if(pendingOnly){
					stmtPending = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER =  ? AND RECEIVE_FLAG = '0'");
					stmtPending.setString(1, customer);
					stmtPending.setString(2, PO);

					ResultSet rsPending = stmtPending.executeQuery();
					while (rsPending.next()) {
						ParScanPOBean data = new ParScanPOBean();
						data.setPoNumber(PO);
						data.setVendor(vendor);
						data.setVendorNumber(vendorNum);
						data.setReceiveFlag("0");
						data.setCreateDate(dateFormat.format(rsPending.getTimestamp("CREATE_DATE")));
						data.setShipTo(rsPending.getString("SHIP_TO"));
						data.setFreeOrderFlag("");
						poList.add(data);
						break;
					}
				
					stmtPending.close();					
				}else{
					receivedFlag = "1";
					shipTo = "";
					createDate = "";
					stmtPending = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER =  ?");
					stmtPending.setString(1, customer);
					stmtPending.setString(2, PO);

					ResultSet rsPending = stmtPending.executeQuery();
					while (rsPending.next()) {
						shipTo = rsPending.getString("SHIP_TO");
						createDate = dateFormat.format(rsPending.getTimestamp("CREATE_DATE"));
						if(rsPending.getString("RECEIVE_FLAG").equalsIgnoreCase("0")){
							receivedFlag = "0";
							break;
						}
					}
					ParScanPOBean data = new ParScanPOBean();
					data.setPoNumber(PO);
					data.setVendor(vendor);
					data.setReceiveFlag(receivedFlag);
					data.setCreateDate(createDate);
					data.setShipTo(shipTo);
					data.setFreeOrderFlag("");
					poList.add(data);
					stmtPending.close();					
				}
			}
			stmt.close();
			
			//Get free orders
			stmt = conn.prepareStatement("SELECT DISTINCT PO_NUMBER FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND CREATE_DATE BETWEEN ? AND ?");
			stmt.setString(1, customer);
			stmt.setTimestamp(2, sqlFromDate);
			stmt.setTimestamp(3, sqlToDate);

			rs = stmt.executeQuery();
			vendorGUID = "";
			vendor = "";
			PO = "";
			shipTo = "";
			createDate = "";
			receivedFlag = "1";
			
			while (rs.next()) {
				PO = rs.getString("PO_NUMBER");				
				stmtVendor = conn.prepareStatement("SELECT DISTINCT VENDOR_GUID FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, rs.getString("PO_NUMBER"));
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				vendorGUID = rsVendor.getString("VENDOR_GUID");
				stmtVendor.close();
				
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, vendorGUID);
				rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				vendor = rsVendor.getString("VENDOR_NAME");
				stmtVendor.close();
				
				if(pendingOnly){
					stmtPending = conn.prepareStatement("SELECT * FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER =  ? AND RECEIVE_FLAG = '0'");
					stmtPending.setString(1, customer);
					stmtPending.setString(2, PO);

					ResultSet rsFreePending = stmtPending.executeQuery();
					while (rsFreePending.next()) {
						ParScanPOBean data = new ParScanPOBean();
						data.setPoNumber(PO);
						data.setVendor(vendor);
						data.setReceiveFlag("0");
						data.setCreateDate(dateFormat.format(rsFreePending.getTimestamp("CREATE_DATE")));
						data.setShipTo(rsFreePending.getString("SHIP_TO"));
						data.setFreeOrderFlag("X");
						poList.add(data);
						break;
					}
				
					stmtPending.close();					
				}else{
					receivedFlag = "1";
					shipTo = "";
					createDate = "";
					stmtPending = conn.prepareStatement("SELECT * FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER =  ?");
					stmtPending.setString(1, customer);
					stmtPending.setString(2, PO);

					ResultSet rsFreePending = stmtPending.executeQuery();
					while (rsFreePending.next()) {
						shipTo = rsFreePending.getString("SHIP_TO");
						createDate = dateFormat.format(rsFreePending.getTimestamp("CREATE_DATE"));
						if(rsFreePending.getString("RECEIVE_FLAG").equalsIgnoreCase("0")){
							receivedFlag = "0";
							break;
						}
					}
					ParScanPOBean data = new ParScanPOBean();
					data.setPoNumber(PO);
					data.setVendor(vendor);
					data.setReceiveFlag(receivedFlag);
					data.setCreateDate(createDate);
					data.setShipTo(shipTo);
					data.setFreeOrderFlag("X");
					poList.add(data);
					stmtPending.close();					
				}
			}
			stmt.close();			
			
			return poList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getPOList: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public List getPODetailsList(String customer, String po) throws ParScanResidentsEJBException {
		
		List poList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtItem;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? ORDER BY ITEM_ID");
			stmt.setString(1, customer);
			stmt.setString(2, po);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				ParScanPOBean data = new ParScanPOBean();

				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();

				data.setVendor(rsVendor.getString("VENDOR_NAME"));
				data.setOrderCost(rs.getString("COST"));
				data.setParArea(rs.getString("PAR_AREA"));
				data.setOrderQuantity(rs.getString("QUANTITY"));
				data.setReceiveQuantity(rs.getString("RECEIVE_QUANTITY"));
				data.setOrderTotal(Double.toString(Double.parseDouble(data.getOrderCost())*Double.parseDouble(data.getOrderQuantity())));
				data.setCreateDate(dateFormat.format(rs.getTimestamp("CREATE_DATE")));
				data.setReceiveFlag(rs.getString("RECEIVE_FLAG"));
				data.setUOM(rs.getString("UOM"));
				if(rs.getTimestamp("RECEIVE_DATE") != null)
					data.setReceiveDate(dateFormat.format(rs.getTimestamp("RECEIVE_DATE")));
				else
					data.setReceiveDate("");
				data.setProductGUID(rs.getString("ITEM_GUID"));

				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1, customer);
				stmtItem.setString(2, data.getProductGUID());
			
				ResultSet rsItem = stmtItem.executeQuery();
				
				while(rsItem.next()){
					data.setProductID(rsItem.getString("ITEM_ID"));
					data.setDescription(rsItem.getString("DESCRIPTION"));					
					data.setCasePakcaging(rsItem.getString("CASE_PACKAGING"));
					data.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
				}
				
				data.setSummary(buildPOItemSummary(customer, po, data));
				
				stmtVendor.close();
				stmtItem.close();
				poList.add(data);
			}
			stmt.close();

			return poList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getPODetailsList: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public List getFreeOrderDetails(String customer, String po) throws ParScanResidentsEJBException{
		List poList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? ORDER BY ITEM");
			stmt.setString(1, customer);
			stmt.setString(2, po);

			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ParScanPOBean data = new ParScanPOBean();

				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();

				data.setVendor(rsVendor.getString("VENDOR_NAME"));
				data.setOrderCost("");
				if(rs.getString("COST") != null)
					data.setOrderCost(rs.getString("COST"));
				data.setParArea("FREE TYPE ORDER");
				data.setOrderQuantity(rs.getString("QUANTITY"));
				data.setReceiveQuantity(rs.getString("RECEIVE_QUANTITY"));
				data.setOrderTotal("");
				if(rs.getString("COST") != null)
					data.setOrderTotal(Double.toString(Double.parseDouble(data.getOrderCost())*Double.parseDouble(data.getOrderQuantity())));
				data.setCreateDate(dateFormat.format(rs.getTimestamp("CREATE_DATE")));
				data.setReceiveFlag(rs.getString("RECEIVE_FLAG"));
				if(rs.getTimestamp("RECEIVE_DATE") != null)
					data.setReceiveDate(dateFormat.format(rs.getTimestamp("RECEIVE_DATE")));
				else
					data.setReceiveDate("");
				data.setProductGUID(rs.getString("ID"));
				data.setProductID(rs.getString("ITEM"));
				data.setDescription("");
				if(rs.getString("DESCRIPTION") != null)
					data.setDescription(rs.getString("DESCRIPTION"));
				data.setUOM(rs.getString("UOM"));
				data.setCasePakcaging("");
				data.setVendorNumber(rs.getString("ITEM"));				
				data.setSummary("");
				
				stmtVendor.close();
				poList.add(data);
			}
			stmt.close();

			return poList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getFreeOrderDetails: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	private String buildPOItemSummary(String customer, String po, ParScanPOBean item)throws ParScanResidentsEJBException{
		String summary = "";
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtItem;
		Connection conn = null;

		try {
			DecimalFormat twoDForm = new DecimalFormat("#.##");
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ITEM_ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, po);
			stmt.setString(3, item.getProductID());

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if(rs.getInt("CNT") > 1){
				stmt.close();
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ITEM_ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, po);
				stmt.setString(3, item.getProductID());				
				rs = stmt.executeQuery();
				
				double orderQty = 0;
				while(rs.next()){
					orderQty = orderQty + Double.parseDouble(rs.getString("QUANTITY"));	
				}								
				
				double total = Double.parseDouble(item.getOrderCost()) * orderQty;				
				summary = "&nbsp &nbsp &nbsp &nbsp Total Order Quantity: " + Integer.toString((int)orderQty) + "&nbsp &nbsp &nbsp &nbsp UOM: " + item.getUOM() + "&nbsp &nbsp &nbsp &nbsp Cost: $" + item.getOrderCost() + "&nbsp &nbsp &nbsp &nbsp TOTAL: $" + twoDForm.format(total);
			}
			stmt.close();

			return summary;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during buildPOItemSummary: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
	
	/**
	 * Business Method.
	 */
	public void removePOItems(String customer, String poNumber, List itemList, String freeOrderType)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();
			
			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
				if(freeOrderType.equalsIgnoreCase("X")){
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ID = ?");
				}else{
					stmt = conn.prepareStatement("DELETE FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ITEM_GUID = ?");
				}				

				stmt.setString(1, customer);
				stmt.setString(2, poNumber);
				stmt.setString(3, data.getProductGUID());

				stmt.executeUpdate();
				stmt.close();
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removePOItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void receiveSelectedItems(String customer, String poNumber, List itemList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtUpdate;
		PreparedStatement stmtStock;
		PreparedStatement stmtItem;
		PreparedStatement stmtKit;
		Map keyMap = new HashMap();
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			
			java.util.Date utilDate = new java.util.Date();
			Timestamp todayS = new Timestamp(utilDate.getTime());
			Timestamp todayStamp = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());
			
			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanFillUpBean data = (ParScanFillUpBean) itor.next();

				if(Integer.parseInt(data.getReceiveQty()) > 0){
					double orderQty = Integer.parseInt(data.getOrderQty());				
					
					if(orderQty == Double.parseDouble(data.getReceiveQty())){
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0' AND ITEM_GUID = ?");
						stmt.setString(1, customer);
						stmt.setString(2, poNumber);
						stmt.setString(3, data.getItemGuid());
						ResultSet rs = stmt.executeQuery();
						rs.next();

						updateStock(customer, poNumber, rs.getString("PAR_AREA"), data.getItemGuid(), data.getReceiveQty(), rs.getString("UOM"), data.getItemID());
						
						stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND ITEM_GUID = ?");
						stmtKit.setString(1, customer);
						stmtKit.setString(2, data.getItemGuid());
						ResultSet rsKit = stmtKit.executeQuery();				
						if(rsKit.next()){
							String kitID = rsKit.getString("KIT_ID");
							double kitQty = Double.parseDouble(rsKit.getString("KIT_QUANTITY"));
							stmtKit.close();
							stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
							stmtKit.setString(1, customer);
							stmtKit.setString(2, rs.getString("PAR_AREA"));
							stmtKit.setString(3, kitID);
							rsKit = stmtKit.executeQuery();
					
							if(rsKit.next()){
								String key = (String) keyMap.get(kitID);
						
								if(key == null){
									keyMap.put(kitID, kitID);
									updateStock(customer, poNumber, rs.getString("PAR_AREA"), kitID, Integer.toString((int)Math.ceil(Double.parseDouble(data.getReceiveQty())*kitQty)), rsKit.getString("UOM"), rsKit.getString("ITEM_ID"));
								}
							}										
						}
						stmtKit.close();						
							
						UID uid = new UID();			
						stmtUpdate = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
						stmtUpdate.setString(1, uid.toString());
						stmtUpdate.setString(2, customer);
						stmtUpdate.setString(3,data.getItemGuid());
						stmtUpdate.setString(4,rs.getString("PAR_AREA"));
						stmtUpdate.setString(5,data.getReceiveQty());
						stmtUpdate.setString(6,rs.getString("UOM"));
						stmtUpdate.setString(7, "Received PO: " + poNumber);	
						stmtUpdate.setTimestamp(8, todayS);	
						stmtUpdate.setString(9, "+");	
						stmtUpdate.executeUpdate();
						stmtUpdate.close();							
						//stmtItem.close();			
						stmt.close();
						stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = QUANTITY WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0' AND ITEM_GUID = ? AND PAR_AREA = ?");
						stmt.setString(1, "1");				
						stmt.setTimestamp(2, todayStamp);
						stmt.setString(3, user);
						stmt.setString(4, customer);
						stmt.setString(5, poNumber);
						stmt.setString(6, data.getItemGuid());
						stmt.setString(7, data.getParArea());
						stmt.executeUpdate();
						stmt.close();							
					}else{
						int recQty = 0;
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0' AND ITEM_GUID = ?");
						stmt.setString(1, customer);
						stmt.setString(2, poNumber);
						stmt.setString(3, data.getItemGuid());
						ResultSet rs = stmt.executeQuery();
						rs.next();
						recQty = Integer.parseInt(rs.getString("RECEIVE_QUANTITY"));

						updateStock(customer, poNumber, rs.getString("PAR_AREA"), data.getItemGuid(), data.getReceiveQty(), rs.getString("UOM"), data.getItemID());	
						
						stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND ITEM_GUID = ?");
						stmtKit.setString(1, customer);
						stmtKit.setString(2, data.getItemGuid());
						ResultSet rsKit = stmtKit.executeQuery();				
						if(rsKit.next()){
							String kitID = rsKit.getString("KIT_ID");
							double kitQty = Integer.parseInt(rsKit.getString("KIT_QUANTITY"));
							stmtKit.close();
							stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
							stmtKit.setString(1, customer);
							stmtKit.setString(2, rs.getString("PAR_AREA"));
							stmtKit.setString(3, kitID);
							rsKit = stmtKit.executeQuery();
					
							if(rsKit.next()){
								String key = (String) keyMap.get(kitID);
						
								if(key == null){
									keyMap.put(kitID, kitID);
									updateStock(customer, poNumber, rs.getString("PAR_AREA"), kitID, Integer.toString((int)Math.ceil(Double.parseDouble(data.getReceiveQty())*kitQty)), rsKit.getString("UOM"), rsKit.getString("ITEM_ID"));
								}
							}										
						}
						stmtKit.close();						
						
						UID uid = new UID();			
						stmtUpdate = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
						stmtUpdate.setString(1, uid.toString());
						stmtUpdate.setString(2, customer);
						stmtUpdate.setString(3,data.getItemGuid());
						stmtUpdate.setString(4,rs.getString("PAR_AREA"));
						stmtUpdate.setString(5,data.getReceiveQty());
						stmtUpdate.setString(6,rs.getString("UOM"));
						stmtUpdate.setString(7, "Received Item from PO: " + poNumber);	
						stmtUpdate.setTimestamp(8, todayS);	
						stmtUpdate.setString(9, "+");	
						stmtUpdate.executeUpdate();
						stmtUpdate.close();							
						//stmtItem.close();			
						stmt.close();
						
						recQty = recQty + Integer.parseInt(data.getReceiveQty());
						
						stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET RECEIVE_QUANTITY = ? WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0' AND ITEM_GUID = ? AND PAR_AREA = ?");
						stmt.setString(1, Integer.toString(recQty));				
						stmt.setString(2, customer);
						stmt.setString(3, poNumber);
						stmt.setString(4, data.getItemGuid());
						stmt.setString(5, data.getParArea());
						stmt.executeUpdate();
						stmt.close();						
					}
				}				
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during receiveSelectedItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	

	/**
	 * Business Method.
	 */
	public void receiveFreeOrderSelectedItems(String customer, String poNumber, List itemList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			
			java.util.Date utilDate = new java.util.Date();
			Timestamp todayStamp = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());
			
			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanFillUpBean data = (ParScanFillUpBean) itor.next();
				
				if(Integer.parseInt(data.getReceiveQty()) > 0){
					double orderQty = Integer.parseInt(data.getOrderQty());				
				
					if(orderQty == Double.parseDouble(data.getReceiveQty())){
						stmt = conn.prepareStatement("UPDATE PARSCAN_FREE_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = QUANTITY WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0' AND ID = ?");
						stmt.setString(1, "1");				
						stmt.setTimestamp(2, todayStamp);
						stmt.setString(3, user);
						stmt.setString(4, customer);
						stmt.setString(5, poNumber);
						stmt.setString(6, data.getItemGuid());
						stmt.executeUpdate();
						stmt.close();							
					}else{
						int recQty = 0;
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0' AND ITEM_GUID = ?");
						stmt.setString(1, customer);
						stmt.setString(2, poNumber);
						stmt.setString(3, data.getItemGuid());
						ResultSet rs = stmt.executeQuery();
						rs.next();						
						recQty = Integer.parseInt(rs.getString("RECEIVE_QUANTITY")) + Integer.parseInt(data.getReceiveQty());
						stmt.close();
						
						stmt = conn.prepareStatement("UPDATE PARSCAN_FREE_ORDER SET RECEIVE_QUANTITY = ? WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0' AND ID = ?");
						stmt.setString(1, Integer.toString(recQty));				
						stmt.setString(2, customer);
						stmt.setString(3, poNumber);
						stmt.setString(4, data.getItemGuid());
						stmt.executeUpdate();
						stmt.close();						
					}
				}				
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during receiveFreeOrderSelectedItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void receivePO(String customer, String poNumber, String freeOrderType)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtUpdate;
		PreparedStatement stmtItem;
		PreparedStatement stmtKit;
		Connection conn = null;
		Map keyMap = new HashMap();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try {
			conn = openConnection();
			java.util.Date today = new java.util.Date();
			Timestamp todayS = new Timestamp(today.getTime());
			Timestamp todayStamp = new Timestamp(dateFormat.parse(dateFormat.format(today)).getTime());			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();

			if(freeOrderType.equalsIgnoreCase("X")){
				stmt = conn.prepareStatement("UPDATE PARSCAN_FREE_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = QUANTITY WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0'");
				stmt.setString(1, "1");				
				stmt.setTimestamp(2, todayStamp);
				stmt.setString(3, user);
				stmt.setString(4, customer);
				stmt.setString(5, poNumber);
				stmt.executeUpdate();
				stmt.close();				
			}else{
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0'");
				stmt.setString(1, customer);
				stmt.setString(2, poNumber);
				ResultSet rs = stmt.executeQuery();
				while(rs.next()){
//					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
//					stmtItem.setString(1, customer);
//					stmtItem.setString(2, rs.getString("ITEM_GUID"));
//					ResultSet rsItem = stmtItem.executeQuery();
//					rsItem.next();

					updateStock(customer, poNumber, rs.getString("PAR_AREA"), rs.getString("ITEM_GUID"), rs.getString("QUANTITY"), rs.getString("UOM"), rs.getString("ITEM_ID"));
				
					stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND ITEM_GUID = ?");
					stmtKit.setString(1, customer);
					stmtKit.setString(2, rs.getString("ITEM_GUID"));
					ResultSet rsKit = stmtKit.executeQuery();
				
					if(rsKit.next()){
						String kitID = rsKit.getString("KIT_ID");
						int kitQty = Integer.parseInt(rsKit.getString("KIT_QUANTITY"));
						stmtKit.close();
						stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
						stmtKit.setString(1, customer);
						stmtKit.setString(2, rs.getString("PAR_AREA"));
						stmtKit.setString(3, kitID);
						rsKit = stmtKit.executeQuery();
					
						if(rsKit.next()){
							String data = (String) keyMap.get(kitID);
						
							if(data == null){
								keyMap.put(kitID, kitID);
								updateStock(customer, poNumber, rs.getString("PAR_AREA"), kitID, Integer.toString(Integer.parseInt(rs.getString("QUANTITY"))*kitQty), rsKit.getString("UOM"), rsKit.getString("ITEM_ID"));
							}
						}										
					}
					stmtKit.close();
				
					UID uid = new UID();			
					stmtUpdate = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
					stmtUpdate.setString(1, uid.toString());
					stmtUpdate.setString(2, customer);
					stmtUpdate.setString(3,rs.getString("ITEM_GUID"));
					stmtUpdate.setString(4,rs.getString("PAR_AREA"));
					stmtUpdate.setString(5,rs.getString("QUANTITY"));
					stmtUpdate.setString(6,rs.getString("UOM"));
					stmtUpdate.setString(7, "Received PO: " + poNumber);	
					stmtUpdate.setTimestamp(8, todayS);	
					stmtUpdate.setString(9, "+");	
					stmtUpdate.executeUpdate();
					stmtUpdate.close();	
					//stmtItem.close();			
				}
			
				stmt.close();
				stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = QUANTITY WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_FLAG = '0'");
				stmt.setString(1, "1");				
				stmt.setTimestamp(2, todayStamp);
				stmt.setString(3, user);
				stmt.setString(4, customer);
				stmt.setString(5, poNumber);
				stmt.executeUpdate();
				stmt.close();			
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during receivePO: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */
	public void unReceivePO(String customer, String poNumber, String freeOrderType)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtUpdate;
		PreparedStatement stmtItem;
		PreparedStatement stmtKit;
		Connection conn = null;
		Map keyMap = new HashMap();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try {
			conn = openConnection();
			java.util.Date today = new java.util.Date();
			Timestamp todayS = new Timestamp(today.getTime());
			Timestamp todayStamp = new Timestamp(dateFormat.parse(dateFormat.format(today)).getTime());			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();

			if(freeOrderType.equalsIgnoreCase("X")){
				stmt = conn.prepareStatement("UPDATE PARSCAN_FREE_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = ? WHERE CUSTOMER = ? AND PO_NUMBER = ?");
				stmt.setString(1, "0");				
				stmt.setTimestamp(2, null);
				stmt.setString(3, user);
				stmt.setString(4, "0");
				stmt.setString(5, customer);
				stmt.setString(6, poNumber);
				stmt.executeUpdate();
				stmt.close();				
			}else{
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND RECEIVE_QUANTITY <> '0'");
				stmt.setString(1, customer);
				stmt.setString(2, poNumber);
				ResultSet rs = stmt.executeQuery();
				while(rs.next()){
					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmtItem.setString(1, customer);
					stmtItem.setString(2, rs.getString("ITEM_GUID"));
					ResultSet rsItem = stmtItem.executeQuery();
					rsItem.next();

					updateStock(customer, poNumber, rs.getString("PAR_AREA"), rs.getString("ITEM_GUID"), "-"+rs.getString("RECEIVE_QUANTITY"), rsItem.getString("VENDOR_UOM"), rs.getString("ITEM_ID"));
				
					stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND ITEM_GUID = ?");
					stmtKit.setString(1, customer);
					stmtKit.setString(2, rs.getString("ITEM_GUID"));
					ResultSet rsKit = stmtKit.executeQuery();
				
					if(rsKit.next()){
						String kitID = rsKit.getString("KIT_ID");
						int kitQty = Integer.parseInt(rsKit.getString("KIT_QUANTITY"));
						stmtKit.close();
						stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
						stmtKit.setString(1, customer);
						stmtKit.setString(2, rs.getString("PAR_AREA"));
						stmtKit.setString(3, kitID);
						rsKit = stmtKit.executeQuery();
					
						if(rsKit.next()){
							String data = (String) keyMap.get(kitID);
						
							if(data == null){
								keyMap.put(kitID, kitID);
								updateStock(customer, poNumber, rs.getString("PAR_AREA"), kitID, "-"+Integer.toString(Integer.parseInt(rs.getString("RECEIVE_QUANTITY"))*kitQty), rsKit.getString("UOM"), rsKit.getString("ITEM_ID"));
							}
						}										
					}
					stmtKit.close();
				
					UID uid = new UID();			
					stmtUpdate = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
					stmtUpdate.setString(1, uid.toString());
					stmtUpdate.setString(2, customer);
					stmtUpdate.setString(3,rs.getString("ITEM_GUID"));
					stmtUpdate.setString(4,rs.getString("PAR_AREA"));
					stmtUpdate.setString(5,rs.getString("RECEIVE_QUANTITY"));
					stmtUpdate.setString(6,rsItem.getString("VENDOR_UOM"));
					stmtUpdate.setString(7, "Un-Received PO: " + poNumber);	
					stmtUpdate.setTimestamp(8, todayS);	
					stmtUpdate.setString(9, "-");	
					stmtUpdate.executeUpdate();
					stmtUpdate.close();	
					stmtItem.close();			
				}
			
				stmt.close();
				stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = ? WHERE CUSTOMER = ? AND PO_NUMBER = ?");
				stmt.setString(1, "0");				
				stmt.setTimestamp(2, null);
				stmt.setString(3, user);
				stmt.setString(4, "0");
				stmt.setString(5, customer);
				stmt.setString(6, poNumber);
				stmt.executeUpdate();
				stmt.close();			
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during unReceivePO: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void unReceiveSelectedItems(String customer, String poNumber, List itemList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		PreparedStatement stmtUpdate;
		PreparedStatement stmtStock;
		PreparedStatement stmtItem;
		PreparedStatement stmtKit;
		Map keyMap = new HashMap();
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			
			java.util.Date utilDate = new java.util.Date();
			Timestamp todayS = new Timestamp(utilDate.getTime());
			Timestamp todayStamp = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());
			
			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanFillUpBean data = (ParScanFillUpBean) itor.next();
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ITEM_GUID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, poNumber);
				stmt.setString(3, data.getItemGuid());
				ResultSet rs = stmt.executeQuery();
				rs.next();
				
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1, customer);
				stmtItem.setString(2, data.getItemGuid());
				ResultSet rsItem = stmtItem.executeQuery();
				rsItem.next();
	
				updateStock(customer, poNumber, rs.getString("PAR_AREA"), data.getItemGuid(), "-"+data.getReceiveQty(), rsItem.getString("VENDOR_UOM"), data.getItemID());
				
				stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND ITEM_GUID = ?");
				stmtKit.setString(1, customer);
				stmtKit.setString(2, data.getItemGuid());
				ResultSet rsKit = stmtKit.executeQuery();				
				if(rsKit.next()){
					String kitID = rsKit.getString("KIT_ID");
					double kitQty = Double.parseDouble(rsKit.getString("KIT_QUANTITY"));
					stmtKit.close();
					stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
					stmtKit.setString(1, customer);
					stmtKit.setString(2, rs.getString("PAR_AREA"));
					stmtKit.setString(3, kitID);
					rsKit = stmtKit.executeQuery();
			
					if(rsKit.next()){
						String key = (String) keyMap.get(kitID);
				
						if(key == null){
							keyMap.put(kitID, kitID);
							updateStock(customer, poNumber, rs.getString("PAR_AREA"), kitID, "-"+Integer.toString((int)Math.ceil(Double.parseDouble(data.getReceiveQty())*kitQty)), rsKit.getString("UOM"), rsKit.getString("ITEM_ID"));
						}
					}										
				}
				stmtKit.close();						
					
				UID uid = new UID();			
				stmtUpdate = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
				stmtUpdate.setString(1, uid.toString());
				stmtUpdate.setString(2, customer);
				stmtUpdate.setString(3,data.getItemGuid());
				stmtUpdate.setString(4,rs.getString("PAR_AREA"));
				stmtUpdate.setString(5,data.getReceiveQty());
				stmtUpdate.setString(6,rsItem.getString("VENDOR_UOM"));
				stmtUpdate.setString(7, "Un-Received Item from PO: " + poNumber);	
				stmtUpdate.setTimestamp(8, todayS);	
				stmtUpdate.setString(9, "-");	
				stmtUpdate.executeUpdate();
				stmtUpdate.close();							
				stmtItem.close();			
				stmt.close();
				stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = ? WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
				stmt.setString(1, "0");				
				stmt.setTimestamp(2, null);
				stmt.setString(3, user);
				stmt.setString(4, "0");				
				stmt.setString(5, customer);
				stmt.setString(6, poNumber);
				stmt.setString(7, data.getItemGuid());
				stmt.setString(8, data.getParArea());
				stmt.executeUpdate();
				stmt.close();											
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during unReceiveSelectedItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void unReceiveFreeOrderSelectedItems(String customer, String poNumber, List itemList)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			
			java.util.Date utilDate = new java.util.Date();
			Timestamp todayStamp = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());
			
			for (Iterator itor = itemList.iterator(); itor.hasNext();) {
				ParScanFillUpBean data = (ParScanFillUpBean) itor.next();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_FREE_ORDER SET RECEIVE_FLAG = ?, RECEIVE_DATE = ?, PARSCAN_USER = ?, RECEIVE_QUANTITY = ? WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ID = ?");
				stmt.setString(1, "0");				
				stmt.setTimestamp(2, null);
				stmt.setString(3, user);
				stmt.setString(4, "0");
				stmt.setString(5, customer);
				stmt.setString(6, poNumber);
				stmt.setString(7, data.getItemGuid());
				stmt.executeUpdate();
				stmt.close();										
			}

		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during unReceiveFreeOrderSelectedItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	private void updateStock(String customer, String poNumber, String parArea, String itemGUID, String fillUpQty, String vendorUOM, String itemID)throws ParScanResidentsEJBException{
		PreparedStatement stmtStock;
		PreparedStatement stmtItem;
		Connection conn = null;
	
		try {
			conn = openConnection();
			java.util.Date today = new java.util.Date();
			Timestamp todayStamp = new Timestamp(today.getTime());			
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
						
			double currStock = 0;
			String stockUOM = "";

			stmtStock = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ?");
			stmtStock.setString(1, customer);
			stmtStock.setString(2, itemID);
			stmtStock.setString(3, parArea);	
			
			ResultSet rsStock = stmtStock.executeQuery();
			if(rsStock.next()){
				stockUOM = rsStock.getString("UOM");
				if(stockUOM.equalsIgnoreCase(vendorUOM)){
					currStock = Double.parseDouble(rsStock.getString("ON_HAND_QUANTITY")) + Double.parseDouble(fillUpQty);		
				}else{
					//Convert PO QTY to stock UOM to add
					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmtItem.setString(1, customer);
					stmtItem.setString(2, itemGUID);
					ResultSet rsItem = stmtItem.executeQuery();
					rsItem.next();
						
					double fillupQty = Double.parseDouble(fillUpQty) * Double.parseDouble(rsItem.getString("MULTIPLIER"));
					currStock = Double.parseDouble(rsStock.getString("ON_HAND_QUANTITY")) + fillupQty;
					stmtItem.close();
				}
								
				stmtStock.close();
					
				stmtStock = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_ACTION = ?, CHANGE_TIMESTAMP = ?, PARSCAN_USER = ? WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ?");
				if(currStock >= 0)				
					stmtStock.setString(1, Integer.toString((int)Math.ceil(currStock)));
				else
					stmtStock.setString(1, "0");
				if(fillUpQty.indexOf('-') == -1)
					stmtStock.setString(2, "PO: " + poNumber + " received");
				else
					stmtStock.setString(2, "PO: " + poNumber + " un-received");
				stmtStock.setTimestamp(3, todayStamp);
				stmtStock.setString(4, user);
				stmtStock.setString(5, customer);
				stmtStock.setString(6, itemID);
				stmtStock.setString(7, parArea);
				stmtStock.executeUpdate();
				stmtStock.close();			
			}
		}catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateStock: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}
	
	/**
	 * Business Method.
	 */	
	public List getStockLog(ParScanReportMessage msg, String itemGUID)throws ParScanResidentsEJBException
	{
		String methodName = "getStockLog()";
		logger.entering( methodName );
		
		
		List stockList = new ArrayList();

		String sqlQuery = sqlProp.getString( "selectStockLog");
		
		String updateActionCritera = ParScanReportMessage.PAR_AREA_TRANSFER_REPORT.equalsIgnoreCase( msg.getReportType() ) ? 
									" AND psl.UPDATE_ACTION like '" + TRANSFERED_FROM.substring( 0,TRANSFERED_FROM.indexOf(" ") ) + "%'" : ""; 
		sqlQuery = sqlQuery.replaceAll( "#updateActionCritera#", updateActionCritera);
		
		logger.debugT("Report type = " + msg.getReportType() + ", updateActionCritera= " +updateActionCritera + " , sqlQuery =" + sqlQuery);
		logger.debugT("Customer Number = " + msg.getCustomerNumber() + ", Par area= " + msg.getParArea() + " , item guid =" + itemGUID 
						+ ", from date =" + msg.getFromDate() + ", to date = " + msg.getToDate());
		
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try 
		{
			SimpleDateFormat dateTimeFormat = new SimpleDateFormat("MM/dd/yyyy h:mm:ss aa");
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");
			Timestamp sqlStartDate = new Timestamp( msg.getFromDate().getTime() );
			DateTime eDate = new DateTime( msg.getToDate().getTime() );
			eDate = eDate.plusDays(1);				
			Timestamp sqlEndDate = new Timestamp( new java.util.Date( eDate.getMillis() ).getTime() );							
			
			conn = openConnection();
			
			pstmt = conn.prepareStatement( sqlQuery );
			
			pstmt.setString(1, msg.getCustomerNumber() );
			pstmt.setString(2, msg.getParArea() );
			pstmt.setString(3, itemGUID);
			pstmt.setTimestamp(4, sqlStartDate);
			pstmt.setTimestamp(5, sqlEndDate);
			
			rs = pstmt.executeQuery();
			
			while( rs.next() )
			{
				ParScanStockBean data = new ParScanStockBean();
				
				data.setUpdateAction( rs.getString("UPDATE_ACTION") );
				data.setCurrentUOM( rs.getString("BILL_UOM") );
				data.setUpdateDate(dateTimeFormat.format( rs.getTimestamp("UPDATE_TIMESTAMP") ) );
				int multiplier = Integer.parseInt( rs.getString("MULTIPLIER") );
				double cost = Double.parseDouble( rs.getString("CURRENT_COST") ) / multiplier;
				data.setCost( outNumberFormat.format(cost) );
				data.setUpdateQuantity( getUpdateQuantity(rs, data, multiplier) ); 
				
				stockList.add( data );
				
			}
			
		} 
		catch (Exception ex) 
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getStockLog: " + stringWriter.toString());
			
			throw new ParScanResidentsEJBException(ex);
		} 
		finally 
		{
			closeDbResources(conn, pstmt, rs);
			logger.exiting( methodName );
		}
		
		return stockList;	
	}

	private String getUpdateQuantity( ResultSet rs, ParScanStockBean data, int multiplier) throws SQLException
	{
		String updateQuantity = rs.getString("UPDATE_QUANTITY");
		
		if(rs.getString("UPDATE_UOM").equalsIgnoreCase( data.getCurrentUOM() ))
		{
			updateQuantity = rs.getString("UPDATE_SIGN").equalsIgnoreCase("-") ? "-" + updateQuantity : updateQuantity;
		}
		else
		{
			int quantity =  Integer.parseInt( updateQuantity ) * multiplier;
			updateQuantity = rs.getString("UPDATE_SIGN").equalsIgnoreCase("-") ? "-" + quantity : "" + quantity;
		}
		
		return updateQuantity;
	}

	/**
	 * Business Method.
	 */
	public List getOnHandStock(String customer, String itemGUID, String billUOM, int multiplier)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;
		List stockList = new ArrayList();

		try {							
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND ITEM_GUID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, itemGUID);
						
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				ParScanStockBean data = new ParScanStockBean();
				
				data.setParAreaGuid(rs.getString("PAR_AREA"));				
				if(rs.getString("UOM").equalsIgnoreCase(billUOM)){
					data.setCriticalLevel(rs.getString("CRITICAL_LEVEL"));
					data.setParLevel(rs.getString("PAR_LEVEL"));
					data.setOnHandQuantity(rs.getString("ON_HAND_QUANTITY"));			
				}else{
					data.setCriticalLevel(Integer.toString(Integer.parseInt(rs.getString("CRITICAL_LEVEL"))*multiplier));
					data.setParLevel(Integer.toString(Integer.parseInt(rs.getString("PAR_LEVEL"))*multiplier));
					data.setOnHandQuantity(Integer.toString(Integer.parseInt(rs.getString("ON_HAND_QUANTITY"))*multiplier));
				}
				
				stockList.add(data);				
			}						
			stmt.close();
			
			return stockList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getOnHandStock: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}	
	
	/**
	 * Business Method.
	 */	
	public ParScanStockBean getRecommendedPar(String customer, String parArea, String itemGUID, String from, String to)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;
		ParScanStockBean data = new ParScanStockBean();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			int days = daysBetween(dateFormat.parse(from), dateFormat.parse(to));				
			int weeks = (int) Math.floor(days/7);			
			Timestamp sqlStartDate = new Timestamp(dateFormat.parse(from).getTime());
			Timestamp sqlEndDate = new Timestamp(dateFormat.parse(to).getTime());							
			conn = openConnection();
			
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, itemGUID);
			ResultSet rs = stmt.executeQuery();
			rs.next();			
			String billUOM = rs.getString("BILL_UOM");
			int multiplier = Integer.parseInt(rs.getString("MULTIPLIER"));
			stmt.close();
			
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ? AND CREATE_DATE BETWEEN ? AND ?");
			stmt.setString(1, customer);
			stmt.setString(2, parArea);
			stmt.setString(3, itemGUID);
			stmt.setTimestamp(4,sqlStartDate);
			stmt.setTimestamp(5,sqlEndDate);
						
			rs = stmt.executeQuery();
			int orderQty = 0;
			while(rs.next()){			
				orderQty = orderQty + Integer.parseInt(rs.getString("QUANTITY"));			
			}						
			stmt.close();
			
			data.setCurrentUOM(billUOM);	
			data.setOrderQuantity(Integer.toString(orderQty*multiplier));
			orderQty = (int) Math.ceil(orderQty/weeks);
			if(orderQty == 0)
				orderQty = 1;	
			data.setParLevel(Integer.toString(orderQty*multiplier));
			
			return data;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getStockLog: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	private int daysBetween(Date startDate, Date endDate) {
	  Calendar cNow = Calendar.getInstance();
	  Calendar cReturnDate = Calendar.getInstance();
	  cNow.setTime(endDate);
	  cReturnDate.setTime(startDate);
	  setTimeToMidnight(cNow);
	  setTimeToMidnight(cReturnDate);
	  long todayMs = cNow.getTimeInMillis();
	  long returnMs = cReturnDate.getTimeInMillis();
	  long intervalMs = todayMs - returnMs;
	  return millisecondsToDays(intervalMs);
	}	
	
	private int millisecondsToDays(long intervalMs) {
	  return (int) (intervalMs / (1000 * 86400));
	}

	private void setTimeToMidnight(Calendar calendar) {
	  calendar.set(Calendar.HOUR_OF_DAY, 0);
	  calendar.set(Calendar.MINUTE, 0);
	  calendar.set(Calendar.SECOND, 0);
	}	
	
	/**
	 * Business Method.
	 */
	public void removePO(String customer, String poNumber, String freeOrderType)throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			
			java.util.Date utilDate = new java.util.Date();
			java.sql.Date sqlDate = new java.sql.Date(utilDate.getTime());
			
			if(freeOrderType.equalsIgnoreCase("X")){
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
			}else{
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");	
			}			

			stmt.setString(1, customer);
			stmt.setString(2, poNumber);

			stmt.executeUpdate();
			stmt.close();
			
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during removePO: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */	
	public List getResidentPayorDetails(String customer, String residentGUID)throws ParScanResidentsEJBException{
		List payerList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtPayor;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {		
			conn = openConnection();	
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PAYORINFO WHERE CUSTOMER = ? AND RESIDENT_GUID = ? ORDER BY EFFECTIVE_DATE ASC");
			stmt.setString(1, customer);
			stmt.setString(2, residentGUID);

			ResultSet rs = stmt.executeQuery();
			
			while(rs.next()){
				stmtPayor = conn.prepareStatement("SELECT * FROM PARSCAN_PAYORCODES WHERE CUSTOMER = ? AND PAYOR_CODE = ?");
				stmtPayor.setString(1, customer);
				stmtPayor.setString(2, rs.getString("PAYOR_CODE"));
				
				ResultSet rsPayor = stmtPayor.executeQuery();
				rsPayor.next();
				
				ParScanPayorCodeBean data = new ParScanPayorCodeBean();
				
				data.setID(rs.getString("ID"));
				data.setPayorCode(rsPayor.getString("PAYOR_CODE"));
				data.setPayorDescription(rsPayor.getString("DESCRIPTION"));
				if(rs.getTimestamp("EFFECTIVE_DATE") != null)
					data.setEffectiveDate(dateFormat.format(rs.getTimestamp("EFFECTIVE_DATE")));
				else
					data.setEffectiveDate("");
				if(rs.getTimestamp("END_DATE") != null)
					data.setEndDate(dateFormat.format(rs.getTimestamp("END_DATE")));
				else
					data.setEndDate("");	
					
				payerList.add(data);					
				stmtPayor.close();
			}
			
			stmt.close();
			
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getResidentPayorDetails: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
		
		return payerList;
	}
	
	/**
	 * Business Method.
	 */
	public void updatePayorDetails(String customer, String resident, ParScanPayorCodeBean data)
		throws ParScanResidentsEJBException {
		PreparedStatement stmt;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			String user =
				UMFactory
					.getAuthenticator()
					.getLoggedInUser()
					.getUniqueName()
					.toUpperCase();
			conn = openConnection();

			stmt =
				conn.prepareStatement(
					"UPDATE PARSCAN_PAYORINFO SET EFFECTIVE_DATE = ?, END_DATE =?, PARSCAN_USER= ? WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND ID = ?");

			Timestamp sqlEffDate = null;
			Timestamp sqlEndDate = null;

			if(!data.getEffectiveDate().equalsIgnoreCase(""))
				sqlEffDate = new Timestamp(dateFormat.parse(data.getEffectiveDate()).getTime());
			if(!data.getEndDate().equalsIgnoreCase(""))	
				sqlEndDate = new Timestamp(dateFormat.parse(data.getEndDate()).getTime());

			stmt.setTimestamp(1, sqlEffDate);
			stmt.setTimestamp(2, sqlEndDate);
			stmt.setString(3, user);
			stmt.setString(4, customer);
			stmt.setString(5, resident);
			stmt.setString(6, data.getID());

			stmt.executeUpdate();
			stmt.close();
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updatePayorDetails: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */	
	public List getUOM(String customer)throws ParScanResidentsEJBException{
		InitialContext initialContext = null;
		PreparedStatement stmt;
		List uomList = new ArrayList();
		Connection conn = null;		
		String key = "";
		
		try
		{
			initialContext = new InitialContext();
			ApplicationConfigHandlerFactory cfgHandler = (ApplicationConfigHandlerFactory) initialContext.lookup("ApplicationConfiguration");
			Properties appProps = cfgHandler.getApplicationProperties();

			Enumeration e = appProps.propertyNames();

			while (e.hasMoreElements()) {
				key = e.nextElement().toString();
				if(key.indexOf("ARSYSTEM") == -1){
					ParScanUOMBean data = new ParScanUOMBean();	
					data.setUom(key);
					data.setDescription(appProps.getProperty(data.getUom()));
					data.setType("System Defined");
			  	
					uomList.add(data);				
				}
			}			

			conn = openConnection();
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_UOM WHERE CUSTOMER = ?");
			stmt.setString(1, customer);
			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {	
				ParScanUOMBean data = new ParScanUOMBean();	
				data.setUom(rs.getString("UOM"));
				data.setDescription(rs.getString("DESCRIPTION"));
				data.setType("User Defined");
			  	
				uomList.add(data);			
			}
			stmt.close();

		} catch (Exception e) {
			StringWriter stringWriter = new StringWriter();
			e.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getUOM: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(e);
		}
		finally
		{
			try{
				if (initialContext != null)
					initialContext.close();
					
				if (conn != null)
					conn.close();					
			}catch (Exception ex){
				//swallow
				StringWriter stringWriter = new StringWriter();
				ex.printStackTrace(new PrintWriter(stringWriter));
			}
		}				
		return uomList;
	}
	
	/**
	 * Business Method.
	 */	
	public List getARSystems()throws ParScanResidentsEJBException
	{
		InitialContext initialContext = null;
		List arList = new ArrayList();
		String key = "";
		String systems = "";
		
		try
		{
			initialContext = new InitialContext();
			ApplicationConfigHandlerFactory cfgHandler = (ApplicationConfigHandlerFactory) initialContext.lookup("ApplicationConfiguration");
			Properties appProps = cfgHandler.getApplicationProperties();

			systems = appProps.getProperty("ARSYSTEMS");

			String[] sysArray = null;
			sysArray = systems.split(",");
			
			for (int i = 0; i < sysArray.length; i++) 
			{
				arList.add( sysArray[i] );
			}			

		} 
		catch (Exception e) 
		{
			StringWriter stringWriter = new StringWriter();
			e.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT( "Error during getARSystems: " + stringWriter.toString() );
			
			throw new ParScanResidentsEJBException(e);
		}
		finally
		{
			try
			{
				if (initialContext != null)
					initialContext.close();		
			}
			catch (Exception ex)
			{
				//swallow
				StringWriter stringWriter = new StringWriter();
				ex.printStackTrace(new PrintWriter(stringWriter));
				ParScanResidentsSSBean.logger.errorT("Error in getARSystems while closing initial context: " + stringWriter.toString());
			}
		}				
		return arList;
	}	
		
	/**
	 * Business Method.
	 */
	public void	addProductsPO(String customer, String po, String parArea, List productList)throws ParScanResidentsEJBException{
		PreparedStatement stmtPO;
		PreparedStatement stmt;	
		Connection conn = null;
		String cost = "";
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			java.util.Date utilDate = new java.util.Date();
			Timestamp sqlDate = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());

			for(Iterator itor = productList.iterator();itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
				stmt = conn.prepareStatement("SELECT COUNT(*) as CNT FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, po);
				stmt.setString(3, parArea);
				stmt.setString(4, data.getProductGUID());
				ResultSet rs = stmt.executeQuery();
				rs.next();
				
				if(rs.getInt("CNT") <= 0){
					stmt.close();
					stmt = conn.prepareStatement("SELECT CURRENT_COST FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, data.getProductGUID());
					rs = stmt.executeQuery();
					rs.next();				
					cost = rs.getString("CURRENT_COST");
					stmt.close();
				
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND PAR_AREA = ?");
					stmt.setString(1, customer);
					stmt.setString(2, po);
					stmt.setString(3, parArea);
					rs = stmt.executeQuery();
					rs.next();
				
					stmtPO = conn.prepareStatement("INSERT INTO PARSCAN_ORDER (ID, CUSTOMER, PO_NUMBER, ITEM_GUID, VENDOR_GUID, QUANTITY, COST, CREATE_DATE, SHIP_TO, PARSCAN_USER, ITEM_ID, PAR_AREA, RECEIVE_FLAG, SENT_MEDLINE, RECEIVE_QUANTITY, UOM) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					UID uid = new UID();									
					stmtPO.setString(1, uid.toString());
					stmtPO.setString(2, customer);
					stmtPO.setString(3, po);
					stmtPO.setString(4,data.getProductGUID());
					stmtPO.setString(5, rs.getString("VENDOR_GUID"));
					stmtPO.setString(6,data.getOrderQuantity());
					stmtPO.setString(7, cost);
					stmtPO.setTimestamp(8, sqlDate);
					stmtPO.setString(9, rs.getString("SHIP_TO"));
					stmtPO.setString(10, user);
					stmtPO.setString(11, data.getProductID());
					stmtPO.setString(12, parArea);
					stmtPO.setString(13, "0");
					stmtPO.setString(14, "0");
					stmtPO.setString(15,"0");				
					stmtPO.setString(16,data.getUOM());
					stmtPO.executeUpdate();
					stmtPO.close();														
				}	
				stmt.close();				
			}

		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during addProductsPO: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
		
	/**
	 * Business Method.
	 */
	public List getParAreaOrderItems(String customer, String parArea, String po)throws ParScanResidentsEJBException{
		List productList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		String vGuid = "";
		String vendor = "";						
		Connection conn = null;
		
		try{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT VENDOR_GUID FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
			stmt.setString(1,customer);
			stmt.setString(2,po);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			vGuid = rs.getString("VENDOR_GUID");
			stmt.close();
			
			stmt = conn.prepareStatement("SELECT VENDOR_NAME FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1,customer);
			stmt.setString(2,vGuid);
			rs = stmt.executeQuery();
			rs.next();
			vendor = rs.getString("VENDOR_NAME");
			stmt.close();			
			
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? ORDER BY ITEM_ID");
			stmt.setString(1,customer);
			stmt.setString(2,parArea);
			rs = stmt.executeQuery();
									
			while (rs.next()) {
				ParScanPOBean data = new ParScanPOBean();
						
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1,customer);
				stmtItem.setString(2,rs.getString("ITEM_GUID"));
				
				ResultSet rsProduct = stmtItem.executeQuery();
				
				if(rsProduct.next()){
					if(rsProduct.getString("VENDOR_ID") != null){
						if(rsProduct.getString("VENDOR_ID").equalsIgnoreCase(vGuid)){
							data.setProductGUID(rs.getString("ITEM_GUID"));
							data.setProductID(rsProduct.getString("ITEM_ID"));
							data.setVendorNumber(rsProduct.getString("VENDOR_ITEM"));
							data.setVendor(vendor);
							data.setUOM(rsProduct.getString("VENDOR_UOM"));
							data.setCasePakcaging(rsProduct.getString("CASE_PACKAGING"));
							data.setDescription(rsProduct.getString("DESCRIPTION"));
					
							productList.add(data);					
						}
					}					
				}
				
				stmtItem.close();
			}
			stmt.close();					
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getParAreaOrderItems: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			

		return productList;
	}	
	
	/**
	 * First it goes over PARSCAN_STOCK table to get all the items for the given customer and par area. Then it gets item details 
	 * for the every item from PARSCAN_ITEM table and returns list of ParScanStockBean
	 */
	public List getParAreaStock(String customer, String parArea)throws ParScanResidentsEJBException{
		
		String methodName = "getParAreaStock()";
		logger.entering(methodName);
		
		List productList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;				
		Connection conn = null;
		String status = "";
		DecimalFormat outNumberFormat = new DecimalFormat("0.00");
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? ORDER BY ITEM_ID");
			stmt.setString(1,customer);
			stmt.setString(2,parArea);
			
			ResultSet rs = stmt.executeQuery();
			
			StringBuffer missingProductBuff = new StringBuffer();
			StringBuffer invalidValueBuffer = new StringBuffer();
			
			while (rs.next()) {
				ParScanStockBean data = new ParScanStockBean();
						
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1,customer);
				stmtItem.setString(2,rs.getString("ITEM_GUID"));
				
				ResultSet rsProduct = stmtItem.executeQuery();
				if(!rsProduct.next()) {
					logger.errorT("PARSCAN_ITEM data not found for CUSTOMER: " + customer + "and ID (ITEM GUID): " + rs.getString("ITEM_GUID"));
					missingProductBuff.append(rs.getString("ITEM_ID")).append(", ");
					continue;
				}
				
				data.setItemGuid(rs.getString("ITEM_GUID"));
				data.setItemID(rsProduct.getString("ITEM_ID"));
				data.setItem(rsProduct.getString("VENDOR_ITEM"));
				data.setItemDescription(rsProduct.getString("DESCRIPTION"));
				data.setCurrentUOM(rs.getString("UOM") == null ? "" : rs.getString("UOM"));
				data.setCasePackaging(rsProduct.getString("CASE_PACKAGING"));
				data.setAlternateBarcode(rsProduct.getString("ALTERNATE_BARCODE"));
				data.setCriticalLevel(rs.getString("CRITICAL_LEVEL") == null ? "" : rs.getString("CRITICAL_LEVEL"));
				data.setParLevel(rs.getString("PAR_LEVEL") == null ? "0" : rs.getString("PAR_LEVEL"));
				data.setOnHandQuantity(rs.getString("ON_HAND_QUANTITY") == null ? "0" : rs.getString("ON_HAND_QUANTITY"));
				data.setUpdateAction(rs.getString("CHANGE_ACTION"));
				data.setStatus(determineStatus(data.getOnHandQuantity(), data.getParLevel()));				
				if(rs.getTimestamp("CHANGE_TIMESTAMP") != null)
					data.setUpdateDate(dateFormat.format(rs.getTimestamp("CHANGE_TIMESTAMP")));
				data.setMedlineFlag(rsProduct.getString("MEDLINE_ITEM"));
				data.setOnOrderQuantity(buildOnOrderString(customer, parArea, data.getItemID(), rsProduct.getString("VENDOR_UOM")));
				
				if(rs.getString("UOM") == null || rs.getString("CRITICAL_LEVEL") == null 
					|| rs.getString("PAR_LEVEL") == null || rs.getString("ON_HAND_QUANTITY") == null) {
					invalidValueBuffer.append(rs.getString("ITEM_ID")).append(", ");
				}
				
				
				if(data.getCurrentUOM().equalsIgnoreCase(rsProduct.getString("BILL_UOM"))){
					if(rsProduct.getString("CURRENT_COST") != null && rsProduct.getString("MULTIPLIER") != null){
						if(!rsProduct.getString("MULTIPLIER").equalsIgnoreCase("0"))
							data.setCost(outNumberFormat.format(Double.parseDouble(rsProduct.getString("CURRENT_COST"))/Double.parseDouble(rsProduct.getString("MULTIPLIER"))));
						else
							data.setCost(rsProduct.getString("CURRENT_COST"));						
					}else{
						data.setCost("0");
					}
				}					
				else{
					if(rsProduct.getString("CURRENT_COST") != null)
						data.setCost(rsProduct.getString("CURRENT_COST"));
					else
						data.setCost("0");						
				}					
				data.setTotalCost(outNumberFormat.format(Double.parseDouble(data.getCost())*Double.parseDouble(data.getOnHandQuantity())));
				
				productList.add(data);
				
				stmtItem.close();
			}
			
			if(productList.size() > 0) {
				if(missingProductBuff.length()>0)					
					missingProductBuff.delete(missingProductBuff.length() -2, missingProductBuff.length() -1);
				if(invalidValueBuffer.length()>0)					
					invalidValueBuffer.delete(invalidValueBuffer.length() -2, invalidValueBuffer.length() -1);
					
				((ParScanStockBean)productList.get(0)).setMissingProducts(missingProductBuff.toString());
				((ParScanStockBean)productList.get(0)).setInvalidValues(invalidValueBuffer.toString());
				
			}
			
			stmt.close();					
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getParAreaStock: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
			
			logger.exiting(methodName);
		}			

		return productList;
	}	
	
	private String buildOnOrderString(String customer, String parArea, String itemID, String UOM)throws ParScanResidentsEJBException{
		PreparedStatement stmt;				
		Connection conn = null;
		String onOrderString = "";
		
		try{
			conn = openConnection();
			
			double onOrder = 0;
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ? AND RECEIVE_FLAG = '0'");
			stmt.setString(1, customer);
			stmt.setString(2, itemID);
			stmt.setString(3, parArea);
			ResultSet rs = stmt.executeQuery();
							
			while(rs.next()){						
				onOrder = onOrder + (Double.parseDouble(rs.getString("QUANTITY")) - Double.parseDouble(rs.getString("RECEIVE_QUANTITY")));
				UOM = rs.getString("UOM");
			}
			stmt.close();
			
			onOrderString = Integer.toString((int)onOrder) + " " + UOM;						
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getOnOrderString: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
		
		return onOrderString;	
	}	
	
	/**
	 * Business Method.
	 */
	public List getParAreaProducts(String customer, String parArea)throws ParScanResidentsEJBException{
		List productList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		PreparedStatement stmtVendor;					
		Connection conn = null;
		String status = "";
		
		try{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ?");
			stmt.setString(1,customer);
			stmt.setString(2,parArea);
			
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()) {
				ParScanItemBean data = new ParScanItemBean();
				String handQty = "";
						
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1,customer);
				stmtItem.setString(2,rs.getString("ITEM_GUID"));
				
				ResultSet rsProduct = stmtItem.executeQuery();
				rsProduct.next();
				
				data.setItemGUID(rs.getString("ITEM_GUID"));
				
				if(rs.getString("UOM").equalsIgnoreCase(rsProduct.getString("BILL_UOM"))){
					handQty = rs.getString("ON_HAND_QUANTITY");
				}else{
					handQty = Double.toString(Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) * Double.parseDouble(rsProduct.getString("MULTIPLIER")));
				}
				
				data.setOnHandQuantity(handQty);
				data.setItemID(rsProduct.getString("ITEM_ID"));
				data.setDescription(rsProduct.getString("DESCRIPTION"));
				data.setBillUom(rsProduct.getString("BILL_UOM"));
				data.setCurrentPrice(rsProduct.getString("CURRENT_PRICE"));
				
				if(rsProduct.getString("VENDOR_ID") != null){
					stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
					stmtVendor.setString(1, customer);
					stmtVendor.setString(2, rsProduct.getString("VENDOR_ID"));
					ResultSet rsVendor = stmtVendor.executeQuery();
					if(rsVendor.next())
						data.setVendor(rsVendor.getString("VENDOR_NAME"));
					else
						data.setVendor("");
						
					stmtVendor.close();					
				}else{
					data.setVendor("");
				}

				
				productList.add(data);
				
				stmtItem.close();				
			}
			stmt.close();					
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getParAreaProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			

		return productList;
	}

	/**
	 * Business Method.
	 */
	public List getItemMaster(String customerNumber, String parArea, String vendor, String category)throws ParScanResidentsEJBException 
	{
		logger.entering( "getItemMaster()" );
		List itemList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCategory;
		PreparedStatement stmtPar;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND VENDOR_PREFERENCE = ?");

			if(!vendor.equalsIgnoreCase("") && !category.equalsIgnoreCase("")){
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND VENDOR_ID = ? AND CATEGORY_ID = ?");
				stmt.setString(2, vendor);
				stmt.setString(3, category);	
			}else if(!vendor.equalsIgnoreCase("") && category.equalsIgnoreCase("")){
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND VENDOR_ID = ?");
				stmt.setString(2, vendor);
			}else if(vendor.equalsIgnoreCase("") && !category.equalsIgnoreCase("")){
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND CATEGORY_ID = ?");
				stmt.setString(2, category);				
			}
			
			stmt.setString(1, customerNumber);
			stmt.setInt(2, 1);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				stmtPar = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
				stmtPar.setString(1, customerNumber);
				stmtPar.setString(2, parArea);
				stmtPar.setString(3, rs.getString("ID"));
				
				ResultSet rsPar = stmtPar.executeQuery();
				rsPar.next();
				
				if(rsPar.getInt("CNT") <= 0){
					stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ?  AND ID = ?");
					String itemVendor = "";
					stmtCategory = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ?  AND ID = ?");
					String itemCategory = "";
					
					if(rs.getString("VENDOR_ID") != null)
						itemVendor = rs.getString("VENDOR_ID");
					if(rs.getString("CATEGORY_ID") != null)
						itemCategory = rs.getString("CATEGORY_ID");						
					
					if(!itemVendor.equalsIgnoreCase("")){						
						stmtVendor.setString(1, customerNumber);
						stmtVendor.setString(2, itemVendor);

						ResultSet rsVendor = stmtVendor.executeQuery();
						rsVendor.next();
						itemVendor = rsVendor.getString("VENDOR_NAME");
					}

					if(!itemCategory.equalsIgnoreCase("")){						
						stmtCategory.setString(1, customerNumber);
						stmtCategory.setString(2, itemCategory);

						ResultSet rsCategory = stmtCategory.executeQuery();
						rsCategory.next();
						itemCategory = rsCategory.getString("PRODUCT_CATEGORY");
					}

					ParScanItemBean data = new ParScanItemBean();

					data.setItemGUID(rs.getString("ID"));
					data.setItemID(rs.getString("ITEM_ID"));
					data.setItem(rs.getString("VENDOR_ITEM"));
					data.setDescription(rs.getString("DESCRIPTION"));
					data.setVendor(itemVendor);
					data.setCategory(itemCategory);
					data.setVendorUom(rs.getString("VENDOR_UOM"));
					data.setBillUom(rs.getString("BILL_UOM"));
					data.setCasePackaging(rs.getString("CASE_PACKAGING"));
					data.setCurrentCost(rs.getString("CURRENT_COST"));
					data.setFutureCost(rs.getString("FUTURE_COST"));
					if(rs.getTimestamp("FUTURE_COST_DATE") != null)
						data.setFutureCostEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_COST_DATE")));
					else
						data.setFutureCostEffectiveDate("");
					data.setCurrentPrice(rs.getString("CURRENT_PRICE"));
					data.setFuturePrice(rs.getString("FUTURE_PRICE"));
					if(rs.getTimestamp("FUTURE_PRICE_DATE") != null)
						data.setFuturePriceEffectiveDate(dateFormat.format(rs.getTimestamp("FUTURE_PRICE_DATE")));
					else
						data.setFuturePriceEffectiveDate("");				
					data.setMultiplier(rs.getString("MULTIPLIER"));
					data.setAlternateBarcode(rs.getString("ALTERNATE_BARCODE"));
					data.setMedlineItem(rs.getString("MEDLINE_ITEM"));
					if(rs.getTimestamp("PRICE_CHANGE_DATE") != null)
						data.setPriceChangeDate(dateFormat.format(rs.getTimestamp("PRICE_CHANGE_DATE")));
					else
						data.setPriceChangeDate("");	
					if(rs.getTimestamp("COST_CHANGE_DATE") != null)
						data.setCostChangeDate(dateFormat.format(rs.getTimestamp("COST_CHANGE_DATE")));
					else
						data.setCostChangeDate("");	
		
					if( "Y".equalsIgnoreCase( rs.getString("FORMULARY_FLAG")) )
					{
						data.setFormularyFlag(true);
					}
					else
					{
						data.setFormularyFlag(false);
					}
					data.setContractFlag("Y".equalsIgnoreCase(rs.getString("CONTRACT_FLAG")));
					itemList.add(data);
					stmtVendor.close();
					stmtCategory.close();				
				}
				stmtPar.close();
			}
			stmt.close();

			return itemList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getItemMaster: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
			logger.exiting( "getItemMaster()" );
		}
	}
	
	/**
	 * Business Method.
	 */	
	public ParScanItemBean getProductUOM(String customer, String product)throws ParScanResidentsEJBException{
		ParScanItemBean data = new ParScanItemBean();
		PreparedStatement stmt;
		Connection conn = null;

		try {
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, product);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				data.setVendorUom(rs.getString("VENDOR_UOM"));
				data.setBillUom(rs.getString("BILL_UOM"));
				data.setMultiplier(rs.getString("MULTIPLIER"));						
			}
			stmt.close();

			return data;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getProductUOM: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}				
	}

	/**
	 * Business Method.
	 * Adds a new par areas for a customer
	 */
	public void editParArea(String customer, String parArea, String editPar, String shipTo) throws ParScanResidentsEJBException{			
			Connection conn = null;
			
			try
			{
				String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
				conn = openConnection();
				PreparedStatement stmt;
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_PARAREA SET PAR_AREA = ?, SHIP_TO = ? WHERE CUSTOMER = ? AND PAR_AREA = ?");
				stmt.setString(1, editPar);
				stmt.setString(2, emptyString(shipTo));
				stmt.setString(3, customer);
				stmt.setString(4, parArea);
				stmt.executeUpdate();
				stmt.close();					
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET PAR_AREA = ? WHERE CUSTOMER = ? AND PAR_AREA = ?");
				stmt.setString(1, editPar);
				stmt.setString(2, customer);
				stmt.setString(3, parArea);
				stmt.executeUpdate();
				stmt.close();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET PAR_AREA = ? WHERE CUSTOMER = ? AND PAR_AREA = ?");
				stmt.setString(1, editPar);
				stmt.setString(2, customer);
				stmt.setString(3, parArea);
				stmt.executeUpdate();
				stmt.close();

				stmt = conn.prepareStatement("UPDATE PARSCAN_RESIDENT SET PAR_AREA = ? WHERE CUSTOMER = ? AND PAR_AREA = ?");
				stmt.setString(1, editPar);
				stmt.setString(2, customer);
				stmt.setString(3, parArea);
				stmt.executeUpdate();
				stmt.close();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK_LOG SET PAR_AREA = ? WHERE CUSTOMER = ? AND PAR_AREA = ?");
				stmt.setString(1, editPar);
				stmt.setString(2, customer);
				stmt.setString(3, parArea);
				stmt.executeUpdate();
				stmt.close();
			}
			catch (Exception ex)
			{
				StringWriter stringWriter = new StringWriter();
				ex.printStackTrace(new PrintWriter(stringWriter));
				ParScanResidentsSSBean.logger.errorT("Error during editParArea: " + stringWriter.toString());
				throw new ParScanResidentsEJBException(ex);
			}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}

	/**
	 * Business Method.
	 * Updates the quantity of products 
	 */
	public void saveParAreaItemChanges(String customer, String parArea, ParScanStockBean data)throws ParScanResidentsEJBException{
		Connection conn = null;
			
		try
		{
			java.util.Date today = new java.util.Date();
			Timestamp todayStamp = new Timestamp(today.getTime());
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
			PreparedStatement stmt;
			PreparedStatement stmtUpdate;
						
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, parArea);
			stmt.setString(3, data.getItemGuid());
			ResultSet rs = stmt.executeQuery();			
			rs.next();			
			
			if(data.getCurrentUOM().equalsIgnoreCase(rs.getString("UOM")) && !data.getOnHandQuantity().equalsIgnoreCase(rs.getString("ON_HAND_QUANTITY"))){
				stmt.close();
				UID uid = new UID();			
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3,data.getItemGuid());
				stmt.setString(4,parArea);
				stmt.setString(5,data.getOnHandQuantity());
				stmt.setString(6,data.getCurrentUOM());
				stmt.setString(7, "Online Count Change");	
				stmt.setTimestamp(8, todayStamp);	
				stmt.setString(9, "*");	
				stmt.executeUpdate();
				stmt.close();			
			}			
						
			stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, PAR_LEVEL = ?, CRITICAL_LEVEL = ?, UOM = ?, CHANGE_ACTION = 'Online product change', CHANGE_TIMESTAMP = ?, PARSCAN_USER = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");			
			stmt.setString(1,data.getOnHandQuantity());
			stmt.setString(2,data.getParLevel());				
			stmt.setString(3,data.getCriticalLevel());	
			stmt.setString(4,data.getCurrentUOM());	
			stmt.setTimestamp(5,todayStamp);
			stmt.setString(6,user);			
			stmt.setString(7,customer);			
			stmt.setString(8, parArea);
			stmt.setString(9, data.getItemGuid());						
			stmt.executeUpdate();
			stmt.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during saveParAreaItemChanges: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}

	/**
	 * Business Method.
	 * Adds new products to a par area for a customer
	 */
	public List addParAreaProducts(String customer, String parArea, List products, boolean updateLevels) throws ParScanResidentsEJBException
	{
		logger.entering("addParAreaProducts()");
		Connection conn = null;
		List newProductList = new ArrayList();
			
		try
		{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
			java.util.Date today = new java.util.Date();
			Timestamp todayStamp = new Timestamp(today.getTime());
			
			PreparedStatement stmtProduct;
			PreparedStatement stmtInsert;
			PreparedStatement stmt;
			PreparedStatement stmtVendor;
			PreparedStatement stmtKit;					
			
			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ?");							
			stmt.setString(1,customer);
			stmt.setString(2,parArea);
		
			ResultSet rs = stmt.executeQuery();
			rs.next();
			
			//check to see if any products are already in Par Area		
			if(rs.getInt("CNT") <= 0)
			{
				stmt.close();
				
				for(Iterator itor = products.iterator();itor.hasNext();)
				{
					ParScanStockBean productData = (ParScanStockBean) itor.next();	
					 
					String vendor_uom = null;
					String multiplier = null;
					String casePackaging = null;
					String issue_uom = null;
					
					if(productData.getCurrentUOM().lastIndexOf("/") == -1 && productData.getCurrentUOM().length() == 2)
					{
						vendor_uom = productData.getCurrentUOM();
						multiplier = Integer.toString((int) getConversion(vendor_uom,productData.getItem()));
						issue_uom = "EA";
					}
					else
					{						
						String[] parsedValues = parseUOMValue( productData.getCurrentUOM() );
						vendor_uom = parsedValues[2];
						multiplier = parsedValues[1];
						issue_uom = parsedValues[0];										 	
					}						
					
					
					stmtProduct = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
					stmtProduct.setString(1, customer);
					stmtProduct.setString(2, parArea);
					stmtProduct.setString(3, productData.getItemGuid());
				
					ResultSet rsProduct = stmtProduct.executeQuery();
					rsProduct.next();
				
					if(rsProduct.getInt("CNT") <= 0)
					{
						stmtProduct.close();	
						stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");						
						stmtVendor.setString(1, customer);
						stmtVendor.setString(2, productData.getItemGuid());
						ResultSet rsVendor = stmtVendor.executeQuery();
						rsVendor.next();
												
						UID uid = new UID(); 
						stmtInsert = conn.prepareStatement("INSERT INTO PARSCAN_STOCK (ID, CUSTOMER, PAR_AREA, ITEM_GUID, ON_HAND_QUANTITY, PAR_LEVEL, CRITICAL_LEVEL, UOM, SCANNED, CHANGE_TIMESTAMP, CHANGE_ACTION, PARSCAN_USER, VENDOR_GUID, ITEM_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
						stmtInsert.setString(1, uid.toString());
						stmtInsert.setString(2, customer);
						stmtInsert.setString(3, parArea);
						stmtInsert.setString(4, productData.getItemGuid());
						stmtInsert.setString(5, productData.getOnHandQuantity());
						stmtInsert.setString(6, productData.getParLevel());
						stmtInsert.setString(7, productData.getCriticalLevel());
						stmtInsert.setString(8, vendor_uom);
						stmtInsert.setString(9, productData.getScanned());
						stmtInsert.setTimestamp(10, todayStamp);
						stmtInsert.setString(11, productData.getUpdateAction());
						stmtInsert.setString(12, user);
						stmtInsert.setString(13, rsVendor.getString("VENDOR_ID"));
						stmtInsert.setString(14, productData.getItemID());					
						stmtInsert.executeUpdate();
						stmtInsert.close();
						
						if(rsVendor.getString("KIT_FLAG") != null)
						{
							stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND KIT_ID = ?");
							stmtKit.setString(1, customer);
							stmtKit.setString(2, productData.getItemID());
							ResultSet rsKit = stmtKit.executeQuery();
							
							while(rsKit.next())
							{
								stmtProduct = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
								stmtProduct.setString(1, customer);
								stmtProduct.setString(2, parArea);
								stmtProduct.setString(3, rsKit.getString("ITEM_GUID"));
				
								rsProduct = stmtProduct.executeQuery();
								rsProduct.next();
				
								if(rsProduct.getInt("CNT") <= 0)
								{
									PreparedStatement parItemStmtProduct = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
									parItemStmtProduct.setString(1, customer);
									parItemStmtProduct.setString(2, rsKit.getString("ITEM_GUID"));
									ResultSet parItemRsProduct = parItemStmtProduct.executeQuery();
									
									if(parItemRsProduct.next())
									{	
										stmtInsert = conn.prepareStatement("INSERT INTO PARSCAN_STOCK (ID, CUSTOMER, PAR_AREA, ITEM_GUID, ON_HAND_QUANTITY, PAR_LEVEL, CRITICAL_LEVEL, UOM, SCANNED, CHANGE_TIMESTAMP, CHANGE_ACTION, PARSCAN_USER, VENDOR_GUID, ITEM_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
										uid = new UID(); 
										stmtInsert.setString(1, uid.toString());
										stmtInsert.setString(2, customer);
										stmtInsert.setString(3, parArea);
										stmtInsert.setString(4, parItemRsProduct.getString("ID"));
										stmtInsert.setString(5, productData.getOnHandQuantity());
										stmtInsert.setString(6, productData.getParLevel());
										stmtInsert.setString(7, productData.getCriticalLevel());
										stmtInsert.setString(8, parItemRsProduct.getString("BILL_UOM"));
										stmtInsert.setString(9, productData.getScanned());
										stmtInsert.setTimestamp(10, todayStamp);
										stmtInsert.setString(11, productData.getUpdateAction());
										stmtInsert.setString(12, user);
										stmtInsert.setString(13, rsVendor.getString("VENDOR_ID"));
										stmtInsert.setString(14, parItemRsProduct.getString("ITEM_ID"));					
										stmtInsert.executeUpdate();
										stmtInsert.close();
									}
									parItemRsProduct.close();						
									parItemStmtProduct.close();			
								}
								stmtProduct.close();							
							}
						}
							
						stmtVendor.close();						
					}else
						stmtProduct.close();					
				}	
			//If not, add only new products and return that list					
			}
			else
			{
				HashMap orgProd = new HashMap();
			
				stmt.close();
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ?");					
				stmt.setString(1,customer);
				stmt.setString(2,parArea);		
			
				ResultSet rsProducts = stmt.executeQuery();
				//Get original product set
				while(rsProducts.next())
				{
					ParScanStockBean prodData = new ParScanStockBean();
			
					prodData.setItemGuid(rsProducts.getString("ITEM_GUID"));
					orgProd.put(rsProducts.getString("ITEM_GUID"), prodData);	
				}
				stmt.close();
						
				for(Iterator itor = products.iterator();itor.hasNext();)
				{
					ParScanStockBean productData = (ParScanStockBean) itor.next();	
					ParScanStockBean data = (ParScanStockBean) orgProd.get(productData.getItemGuid());				
										
					String vendor_uom = null;
					String multiplier = null;
					String casePackaging = null;
					String issue_uom = null;
					
					if(productData.getCurrentUOM().lastIndexOf("/") == -1 && productData.getCurrentUOM().length() == 2)
					{
						vendor_uom = productData.getCurrentUOM();
						multiplier = Integer.toString((int) getConversion(vendor_uom,productData.getItem()));
						issue_uom = "EA";
					}
					else
					{						
						String[] parsedValues = parseUOMValue(productData.getCurrentUOM());
						vendor_uom = parsedValues[2];
						multiplier = parsedValues[1];
						issue_uom = parsedValues[0];										 	
					}					
															
					//Add only new products to list
					if(data == null)
					{	
						stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");	
						stmtVendor.setString(1, customer);
						stmtVendor.setString(2, productData.getItemGuid());
						ResultSet rsVendor = stmtVendor.executeQuery();
						rsVendor.next();						
						
						UID uid = new UID(); 
						
						stmtInsert = conn.prepareStatement("INSERT INTO PARSCAN_STOCK (ID, CUSTOMER, PAR_AREA, ITEM_GUID, ON_HAND_QUANTITY, PAR_LEVEL, CRITICAL_LEVEL, UOM, SCANNED, CHANGE_TIMESTAMP, CHANGE_ACTION, PARSCAN_USER, VENDOR_GUID, ITEM_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
						stmtInsert.setString(1, uid.toString());
						stmtInsert.setString(2, customer);
						stmtInsert.setString(3, parArea);
						stmtInsert.setString(4, productData.getItemGuid());
						stmtInsert.setString(5, productData.getOnHandQuantity());
						stmtInsert.setString(6, productData.getParLevel());
						stmtInsert.setString(7, productData.getCriticalLevel());
						stmtInsert.setString(8, vendor_uom); 
						stmtInsert.setString(9, productData.getScanned());
						stmtInsert.setTimestamp(10, todayStamp);
						stmtInsert.setString(11, productData.getUpdateAction());
						stmtInsert.setString(12, user);
						stmtInsert.setString(13, rsVendor.getString("VENDOR_ID"));
						stmtInsert.setString(14, productData.getItemID());
				
						int insCnt = stmtInsert.executeUpdate();	
						logger.debugT("PARSCAN_STOCK insCnt = "+insCnt); 					
						stmtInsert.close();		
						
						if(rsVendor.getString("KIT_FLAG") != null)
						{
							stmtKit = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND KIT_ID = ?");
							stmtKit.setString(1, customer);
							stmtKit.setString(2, productData.getItemID());
							ResultSet rsKit = stmtKit.executeQuery();
							
							while( rsKit.next() )
							{
								stmtProduct = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
								stmtProduct.setString(1, customer);
								stmtProduct.setString(2, parArea);
								stmtProduct.setString(3, rsKit.getString("ITEM_GUID"));
				
								ResultSet rsProduct = stmtProduct.executeQuery();
								rsProduct.next();
				
								if( rsProduct.getInt("CNT") <= 0 )
								{
									stmtProduct.close();
									stmtProduct = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
									stmtProduct.setString(1, customer);
									stmtProduct.setString(2, rsKit.getString("ITEM_GUID"));
									rsProduct = stmtProduct.executeQuery();
									
									rsProduct.next();
																		
									stmtInsert = conn.prepareStatement("INSERT INTO PARSCAN_STOCK (ID, CUSTOMER, PAR_AREA, ITEM_GUID, ON_HAND_QUANTITY, PAR_LEVEL, CRITICAL_LEVEL, UOM, SCANNED, CHANGE_TIMESTAMP, CHANGE_ACTION, PARSCAN_USER, VENDOR_GUID, ITEM_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
									uid = new UID(); 
									stmtInsert.setString(1, uid.toString());
									stmtInsert.setString(2, customer);
									stmtInsert.setString(3, parArea);
									stmtInsert.setString(4, rsProduct.getString("ID"));
									stmtInsert.setString(5, productData.getOnHandQuantity());
									stmtInsert.setString(6, productData.getParLevel());
									stmtInsert.setString(7, productData.getCriticalLevel());
									stmtInsert.setString(8, rsProduct.getString("BILL_UOM"));
									stmtInsert.setString(9, productData.getScanned());
									stmtInsert.setTimestamp(10, todayStamp);
									stmtInsert.setString(11, productData.getUpdateAction());
									stmtInsert.setString(12, user);
									stmtInsert.setString(13, rsVendor.getString("VENDOR_ID"));
									stmtInsert.setString(14, rsProduct.getString("ITEM_ID"));					
									int insertCnt = stmtInsert.executeUpdate();
									logger.debugT("insertCnt = "+insertCnt);
									stmtInsert.close();									
								}
								stmtProduct.close();							
							}
						}						
						
						stmtVendor.close();								
						newProductList.add(productData);
					}
					else
					{
						if(updateLevels)
						{
							stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, PAR_LEVEL = ?, CRITICAL_LEVEL = ?, UOM = ?, CHANGE_ACTION = 'Online product change', CHANGE_TIMESTAMP = ?, PARSCAN_USER = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");			
							stmt.setString(1,productData.getOnHandQuantity());
							stmt.setString(2,productData.getParLevel());				
							stmt.setString(3,productData.getCriticalLevel());	
							stmt.setString(4,vendor_uom);
							stmt.setTimestamp(5,todayStamp);
							stmt.setString(6,user);			
							stmt.setString(7,customer);			
							stmt.setString(8, parArea);
							stmt.setString(9, productData.getItemGuid());						
							int updCnt = stmt.executeUpdate();
							logger.debugT("updCnt = "+updCnt);
							stmt.close();						
						}						
					}
				}		
			}

			return newProductList;
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during addParAreaProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally
		{
			try
			{
				if (conn != null)conn.close();
			} catch (SQLException e)
			{
				
				e.printStackTrace();
			}
			logger.exiting("addParAreaProducts()");
		}	
	}

	/**
	 * Business Method.
	 * Adds new products to a product master for a customer
	 */
	public List addProductsFromTemplate(String customer, List products) throws ParScanResidentsEJBException
	{
		logger.entering("addProductsFromTemplate");
		Connection conn = null;
					
		try
		{
			DecimalFormat twoDForm = new DecimalFormat("#.##");
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			PreparedStatement stmtProduct;
			PreparedStatement stmtDetail;
			PreparedStatement stmt;
			PreparedStatement stmtVendor;										
						
			for(Iterator itor = products.iterator();itor.hasNext();)
			{
				ParScanStockBean productData = (ParScanStockBean) itor.next();
				
				stmtProduct = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND MEDLINE_ITEM = 'X' AND VENDOR_ITEM = ?");
				stmtProduct.setString(1, customer);
				stmtProduct.setString(2, productData.getItemID());
				
				ResultSet rsProduct = stmtProduct.executeQuery();
				rsProduct.next();
				
				if(rsProduct.getInt("CNT") <= 0)
				{						
					stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
					stmtVendor.setString(1, customer);
					stmtVendor.setString(2, "Medline Industries");
					ResultSet rsVendor = stmtVendor.executeQuery();
					
					rsVendor.next();
					
					stmt = conn.prepareStatement("INSERT INTO PARSCAN_ITEM (ID, CUSTOMER, ITEM_ID, VENDOR_ITEM, DESCRIPTION, VENDOR_ID, CATEGORY_ID, VENDOR_UOM, MULTIPLIER, CASE_PACKAGING, CURRENT_COST, FUTURE_COST, FUTURE_COST_DATE, CURRENT_PRICE, FUTURE_PRICE, FUTURE_PRICE_DATE, ALTERNATE_BARCODE, MEDLINE_ITEM, BILL_UOM, PARSCAN_USER, PRICE_CHANGE_DATE, COST_CHANGE_DATE, VENDOR_PREFERENCE, KIT_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");				
					UID uid = new UID(); 
					productData.setItemGuid(uid.toString());
					Timestamp todayDate = new Timestamp(new java.util.Date().getTime());

					String vendor_uom = null;
					String multiplier = null;
					String casePackaging = null;
					String issue_uom = null;
					
					if( productData.getCurrentUOM().lastIndexOf("/") == -1 && productData.getCurrentUOM().length() == 2 )
					{
						vendor_uom = productData.getCurrentUOM();
						multiplier = Integer.toString( (int) getConversion( vendor_uom,productData.getItem() ) );
						issue_uom = "EA";
					}
					else
					{						
						String[] parsedValues = parseUOMValue( productData.getCurrentUOM() );
						vendor_uom = parsedValues[2];
						multiplier = parsedValues[1];
						issue_uom = parsedValues[0];										 	
					}
									
					stmt.setString(1, uid.toString());
					stmt.setString(2, customer);
					stmt.setString(3, productData.getItemID());
					stmt.setString(4, productData.getItem());
					stmt.setString(5, productData.getItemDescription());
					stmt.setString(6, rsVendor.getString("ID"));
					stmt.setString(7, null);
					stmt.setString(8, vendor_uom);					
					stmt.setString(9, multiplier);					
					
					String cost = getMedlinePrice(customer, productData.getItem());
					casePackaging = Integer.toString((int) getConversion(vendor_uom,productData.getItem())) + " EA/" + vendor_uom;
					stmt.setString(10, casePackaging);
					stmt.setString(11, cost);
					stmt.setString(12, null);
					stmt.setTimestamp(13, null);	
					if(productData.getDefaultMarkup() != 0)
					{
						if(vendor_uom.equalsIgnoreCase("EA"))
						{					
							String newPrice = twoDForm.format(Double.parseDouble(cost) * (productData.getDefaultMarkup()/100 + 1));
							stmt.setString(14, newPrice);							
						}
						else
						{
							double curPrice = Double.parseDouble(cost) / Double.parseDouble(multiplier);					
							String newPrice = twoDForm.format( curPrice * ( productData.getDefaultMarkup()/100 + 1 ) );
							stmt.setString( 14, newPrice );						
						}				
					}
					else
					{
						stmt.setString(14, null);
					}						
					stmt.setString(15, null);				
					stmt.setTimestamp(16, null);
					stmt.setString(17, null);
					stmt.setString(18, "X");
					stmt.setString(19, issue_uom);
					stmt.setString(20, user);
					stmt.setTimestamp(21, null);
					stmt.setTimestamp(22, todayDate);
					stmt.setInt(23, 1);					
					stmt.setString(24, null);
					
					int updateCnt = stmt.executeUpdate();
					
					stmt.close();
					stmtVendor.close();
				}
				else
				{					
					stmtDetail = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND MEDLINE_ITEM = 'X' AND VENDOR_ITEM = ?");
					stmtDetail.setString(1, customer);
					stmtDetail.setString(2, productData.getItemID());
				
					ResultSet rs = stmtDetail.executeQuery();
					rs.next();
					
					productData.setItemGuid(rs.getString("ID"));
					stmtDetail.close();					
				}
				stmtProduct.close();
			}							
			
			return products;
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during addProductsFromTemplate: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally
		{
			try 
			{
				if (conn != null)conn.close();
			} 
			catch (SQLException e) 
			{
				
				e.printStackTrace();
			}
		}	
	}
	
	/**
	 * Business Method.
	 * Removes products from par areas for a customer
	 */
	public void removeParAreaProducts(String customer, String parArea, List products) throws ParScanResidentsEJBException{
		Connection conn = null;
			
		try
		{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
			PreparedStatement stmt;

			for(Iterator itor = products.iterator();itor.hasNext();){
				ParScanStockBean data = (ParScanStockBean) itor.next();
					
				stmt = conn.prepareStatement("DELETE FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
			
				stmt.setString(1, customer);
				stmt.setString(2, parArea);
				stmt.setString(3, data.getItemGuid());
			
				stmt.executeUpdate();
				stmt.close();				
			}			
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during removeParAreaProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	private static String getPath(String productNumber) throws UnsupportedEncodingException
	{
		  String encProductNumber = URLEncoder.encode(productNumber, "UTF-8");
		  String folder = (encProductNumber.length() >= 3) ? encProductNumber.substring(0, 3) : encProductNumber;
		  StringBuffer sb = new StringBuffer();
		  sb.append("/products2/").append(folder);
		  sb.append("/").append(encProductNumber).append(".txt");
		  return sb.toString();
	}	

	public String getDescription(String productNumber) throws ParScanResidentsEJBException
	{
		try{
			com.sapportals.portal.security.usermanagement.IUser user = WPUMFactory.getServiceUserFactory().getServiceUser("cmadmin_service");
			IResourceContext ctx = new ResourceContext(user);
			RID rid = RID.getRID(getPath(productNumber));
			IResource resource = ResourceFactory.getInstance().getResource(rid, ctx);
			
			if (resource != null){
				//get short description
				IProperty prop = resource.getProperty(PropertyName.getPN("http://sapportals.com/xmlns/cm", "med_productdesc"));
			  
				if (prop != null && prop.getValue() != null)
				{
						return prop.getValueAsString();
				}
			}else{
				rid = RID.getRID(getPath(productNumber).replaceAll(".txt",".zip"));
				resource = ResourceFactory.getInstance().getResource(rid, ctx);
				
				if (resource != null){
					//get short description
					IProperty prop = resource.getProperty(PropertyName.getPN("http://sapportals.com/xmlns/cm", "med_productdesc"));
			  
					if (prop != null && prop.getValue() != null)
					{
							return prop.getValueAsString();
					}
				}			
			}
			return "";
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getDescription: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
	}
	
	public String getMedlineUOM(String productNumber) throws ParScanResidentsEJBException
	{
		InitialContext ctx = null;
		try
		{
			String salesUOM = "";
			String[] productNumbers = new String[1];
			productNumbers[0] = productNumber;
		
			ctx = new InitialContext();	
			ZgetProductUnitofMeasuresService packagingservice =
				(ZgetProductUnitofMeasuresService) ctx.lookup(
					"/wsclients/proxies/sap.com/com.medline.product.WebServices/com.medline.packaging.proxy.ZgetProductUnitofMeasures");

			ZgetProductUnitofMeasures productUom = packagingservice.getLogicalPort("ZgetProductUnitofMeasuresSoapBindingBasicAuthentication");
			ProductUOMRequest request = new ProductUOMRequest();
			request.setProductNumber(productNumbers);

			ProductUnitofMeasuresDT[] uomResults = productUom.getProductUnitofMeasures(request);
			
			PackagingDetails[] packagingDetails = null;

			if (uomResults != null)
			{
				packagingDetails = new PackagingDetails[uomResults.length];

				for (int x = 0; x < uomResults.length; x++)
				{

					ProductUnitofMeasuresDT uomResult = uomResults[x];

					PackagingDetails packagingDetail = new PackagingDetails();
					packagingDetails[x] = packagingDetail;
					packagingDetail.setProductNumber(uomResult.getProductNumber());

					if (uomResult.getSalesUnitofMeasure() != null)
					{
						UnitOfMeasure uom = new UnitOfMeasure();
						uom.setDescription(uomResult.getSalesUnitofMeasure().getDescription());
						uom.setUnitOfMeasure(uomResult.getSalesUnitofMeasure().getUnitofMeasure());
						packagingDetail.setSalesUom(uom);
						salesUOM = getNAValue(getSalesUOM(packagingDetail.getSalesUom()));
					}else{
						salesUOM = "N/A";
					}
				}
			}	
			
			return salesUOM;
			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getMedlineUOM: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}					
	}	
	
	/**
	 * Business Method.
	 */
	public List getOrderParAreaList(String customerNumber, String orderLevel, String onlyToday, String ignoreOrder) throws ParScanResidentsEJBException{
		List orderList = new ArrayList();
		List parAreaList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		PreparedStatement stmtPO;		
		PreparedStatement parStmt;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			java.util.Date stockDate = new java.util.Date();
			stockDate = dateFormat.parse(dateFormat.format(stockDate));
			Timestamp todayStamp = new Timestamp(stockDate.getTime());			
			
			conn = openConnection();	
	
			parStmt = conn.prepareStatement("SELECT PAR_AREA, SHIP_TO FROM PARSCAN_PARAREA WHERE CUSTOMER = '" + customerNumber + "' AND PAR_AREA <> '" + customerNumber + " - CONSIGNMENT' ORDER BY PAR_AREA ASC");			
			ResultSet rsPar = parStmt.executeQuery();
		
			while(rsPar.next()){
				ParScanPOBean parData = new ParScanPOBean();
				
				parData.setParArea(rsPar.getString("PAR_AREA"));
				parData.setShipTo(rsPar.getString("SHIP_TO"));
				
				parAreaList.add(parData);
			}
	
			if(orderLevel.equalsIgnoreCase("")){
				orderList = parAreaList;
			}else{
				for(Iterator itor = parAreaList.iterator();itor.hasNext();){				
					ParScanPOBean data = (ParScanPOBean) itor.next();
					int qtyCount = 0;

					if(onlyToday.equalsIgnoreCase("X")){
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = '" + customerNumber + "' AND CHANGE_TIMESTAMP >= ?");
						stmt.setString(1,data.getParArea().toString());
						stmt.setTimestamp(2, todayStamp);						
					}else{
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = '" + customerNumber + "'");
						stmt.setString(1,data.getParArea().toString());						
					}

					ResultSet rs = stmt.executeQuery();
		
					while(rs.next()){
						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmtItem.setString(1, customerNumber);
						stmtItem.setString(2, rs.getString("ITEM_GUID"));
						ResultSet rsItem = stmtItem.executeQuery();
						rsItem.next();
					
						if(rsItem.getString("KIT_FLAG") == null){
							double onOrder = 0;
							if(!ignoreOrder.equalsIgnoreCase("X")){
								stmtPO = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ? AND RECEIVE_FLAG = '0'");
								stmtPO.setString(1, customerNumber);
								stmtPO.setString(2, rs.getString("ITEM_ID"));
								stmtPO.setString(3, data.getParArea());
								ResultSet rsPO = stmtPO.executeQuery();
								
								while(rsPO.next()){						
									//Check if stock UOM is in order UOM
									if(rs.getString("UOM").equalsIgnoreCase(rsItem.getString("VENDOR_UOM")))
										onOrder = onOrder + (Double.parseDouble(rsPO.getString("QUANTITY")) - Double.parseDouble(rsPO.getString("RECEIVE_QUANTITY")));
									else
									//convert order UOM to Stock UOM
										onOrder = onOrder + ((Double.parseDouble(rsPO.getString("QUANTITY"))*Double.parseDouble(rsItem.getString("MULTIPLIER"))) - (Double.parseDouble(rsPO.getString("RECEIVE_QUANTITY"))*Double.parseDouble(rsItem.getString("MULTIPLIER"))));
								}
								stmtPO.close();	
							}
										
							if(orderLevel.equalsIgnoreCase("critical")){
								int orderQty = (int)Math.ceil(Double.parseDouble(rs.getString("CRITICAL_LEVEL")) - (Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) + onOrder));

								if(orderQty >= 0 && Double.parseDouble(rs.getString("PAR_LEVEL")) > 0){
									qtyCount = qtyCount + 1;	
								}										
							}else{
								int orderQty = (int)Math.ceil(Double.parseDouble(rs.getString("PAR_LEVEL")) - (Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) + onOrder));

								if(orderQty > 0){
									qtyCount = qtyCount + 1;	
								}					
							}						
						}	
						stmtItem.close();						
					}
					stmt.close();
				
					if(qtyCount > 0){
						ParScanPOBean orderData = new ParScanPOBean();
					
						orderData.setShipTo(data.getShipTo());
						orderData.setParArea(data.getParArea());
						orderData.setProductCount(Integer.toString(qtyCount));
					
						orderList.add(orderData);
					}
				}				
			}

			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getOrderParAreaList: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		

		return orderList;
	}	

	/**
	 * Business Method.
	 */		
	public List getPOMultipleIDs(String customer, String orderLevel, List parAreaList) throws ParScanResidentsEJBException{
		List idList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;
		Map idMap = new HashMap();
			
		try{
			conn = openConnection();				
	
			for(Iterator itor = parAreaList.iterator();itor.hasNext();){
				ParScanPOBean parData = (ParScanPOBean) itor.next();
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = ?");				
				stmt.setString(1, parData.getParArea());
				stmt.setString(2,customer);
									
				ResultSet rs = stmt.executeQuery();
				
				while(rs.next()){
					if(orderLevel.equalsIgnoreCase("critical")){
						int orderQty = Integer.parseInt(rs.getString("CRITICAL_LEVEL")) - Integer.parseInt(rs.getString("ON_HAND_QUANTITY"));

						if(orderQty >= 0 && Integer.parseInt(rs.getString("PAR_LEVEL")) > 0){
							ParScanItemBean data = new ParScanItemBean();								
							stmtItem = conn.prepareStatement("SELECT DISTINCT ITEM_ID FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
							stmtItem.setString(1, customer);
							stmtItem.setString(2, rs.getString("ITEM_GUID"));
							ResultSet rsItem = stmtItem.executeQuery();
							rsItem.next();
							
							data.setItemID(rsItem.getString("ITEM_ID"));

							stmtItem.close();
							
							stmtItem = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
							stmtItem.setString(1, customer);
							stmtItem.setString(2, data.getItemID());				

							ResultSet rsMulti = stmtItem.executeQuery();
							rsMulti.next();

							if (rsMulti.getInt("CNT") > 1){
								ParScanItemBean idData = (ParScanItemBean) idMap.get(data.getItemID());
			
								if(idData == null){
									idMap.put(data.getItemID(), data);
									idList.add(data);
								}
							}
							stmtItem.close();						
						}				
					}else{
						int orderQty = Integer.parseInt(rs.getString("PAR_LEVEL")) - Integer.parseInt(rs.getString("ON_HAND_QUANTITY"));
						if(orderQty > 0){
							ParScanItemBean data = new ParScanItemBean();								
							stmtItem = conn.prepareStatement("SELECT DISTINCT ITEM_ID FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
							stmtItem.setString(1, customer);
							stmtItem.setString(2, rs.getString("ITEM_GUID"));
							ResultSet rsItem = stmtItem.executeQuery();
							rsItem.next();
							
							data.setItemID(rsItem.getString("ITEM_ID"));
							stmtItem.close();
							
							stmtItem = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ?");
							stmtItem.setString(1, customer);
							stmtItem.setString(2, data.getItemID());				

							ResultSet rsMulti = stmtItem.executeQuery();
							rsMulti.next();
							if (rsMulti.getInt("CNT") > 1){
								ParScanItemBean idData = (ParScanItemBean) idMap.get(data.getItemID());
			
								if(idData == null){
									idMap.put(data.getItemID(), data);
									idList.add(data);
								}
							}
							stmtItem.close();
						}												
					}			
				}
				stmt.close();					
			}
			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getPOMultipleIDs: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	
		return idList;
	}	
	
	/**
	 * Business Method.
	 */
	public void updateOrderVendorPreference(String customer, List preferredItems, List parAreaList)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;
			
		try{
			conn = openConnection();	

			for(Iterator itorItem = preferredItems.iterator();itorItem.hasNext();){
				ParScanItemBean itemData = (ParScanItemBean) itorItem.next();
				
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND VENDOR_PREFERENCE = ? AND ITEM_ID = ?");
				stmtItem.setString(1, customer);
				stmtItem.setInt(2, 1);
				stmtItem.setString(3, itemData.getItemID());
				ResultSet rsItem = stmtItem.executeQuery();
				rsItem.next();			
				String oldGUID = rsItem.getString("ID");

				stmtItem.close();
				
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1, customer);
				stmtItem.setString(2, itemData.getItemGUID());
				rsItem = stmtItem.executeQuery();
				rsItem.next();
				String newVendorGUID = rsItem.getString("VENDOR_ID");
				stmtItem.close();									
		
				for(Iterator itor = parAreaList.iterator();itor.hasNext();){
					ParScanPOBean parData = (ParScanPOBean) itor.next();
					
					stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ITEM_GUID = ?, VENDOR_GUID = ? WHERE PAR_AREA = ? AND CUSTOMER = ? AND ITEM_GUID = ? AND SCANNED <> 'N'");					

					stmt.setString(1, itemData.getItemGUID());
					stmt.setString(2, newVendorGUID);				
					stmt.setString(3, parData.getParArea());
					stmt.setString(4,customer);
					stmt.setString(5,oldGUID);
					stmt.executeUpdate();				
					stmt.close();					
				}
			}
			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during updateOrderVendorPreference: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}	
	
	/**
	 * Business Method.
	 */		
	public void resetPreferredStock(String customer, List parAreaList, List itemIDs)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		Connection conn = null;
				
		try{
			conn = openConnection();	

			for(Iterator itor = itemIDs.iterator();itor.hasNext();){
				ParScanItemBean idData = (ParScanItemBean) itor.next();
				
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ITEM_ID = ? AND VENDOR_PREFERENCE = ?");
				stmtItem.setString(1, customer);
				stmtItem.setString(2, idData.getItemID());
				stmtItem.setInt(3,1);
				
				ResultSet rs = stmtItem.executeQuery();
				if(rs.next()){
					String preferredGUID = rs.getString("ID");
					String preferredVendor = rs.getString("VENDOR_ID");

					for(Iterator parItor = parAreaList.iterator();parItor.hasNext();){
						ParScanPOBean parData = (ParScanPOBean) parItor.next();
				
						stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ITEM_GUID = ?, VENDOR_GUID = ? WHERE PAR_AREA = ? AND CUSTOMER = ? AND ITEM_GUID = ? AND SCANNED <> 'N'");
						stmt.setString(1, preferredGUID);
						stmt.setString(2, preferredVendor);				
						stmt.setString(3, parData.getParArea());
						stmt.setString(4,customer);
						stmt.setString(5,idData.getItemGUID());
						stmt.executeUpdate();				
						stmt.close();											
					}				
				}										
				stmtItem.close();	
			}		
			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during resetPreferredStock: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}		
	
	/**
	 * Business Method.
	 */	
	public List getPONumberData(String customer, String orderLevel, String onlyToday, List parAreaList, String ignoreOpen) throws ParScanResidentsEJBException{
		List poList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement vendorStmt;	
		PreparedStatement stmtItem;
		PreparedStatement stmtPO;
		Connection conn = null;
		Map vendorMap = new HashMap();
		Map shipMap = new HashMap();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			java.util.Date stockDate = new java.util.Date();
			stockDate = dateFormat.parse(dateFormat.format(stockDate));
			Timestamp todayStamp = new Timestamp(stockDate.getTime());
			
			conn = openConnection();							

			for(Iterator itor = parAreaList.iterator();itor.hasNext();){
				if(onlyToday.equalsIgnoreCase("X")){
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = ? AND CHANGE_TIMESTAMP >= ?");						
				}else{
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = ?");						
				}
								
				ParScanPOBean parData = (ParScanPOBean) itor.next();			
				String shipToData = (String) shipMap.get(parData.getShipTo());
			
				if(shipToData == null){
					shipMap.put(parData.getShipTo(), parData.getShipTo());
					vendorMap.clear();
				}				
			
				stmt.setString(1, parData.getParArea());
				stmt.setString(2,customer);
				if(onlyToday.equalsIgnoreCase("X"))
					stmt.setTimestamp(3, todayStamp);
				
				ResultSet rs = stmt.executeQuery();
			
				while(rs.next()){
					if(rs.getString("VENDOR_GUID") != null){
						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmtItem.setString(1, customer);
						stmtItem.setString(2, rs.getString("ITEM_GUID"));
						ResultSet rsItem = stmtItem.executeQuery();
						rsItem.next();
						
						if(rsItem.getString("KIT_FLAG") == null){
							int onOrder = 0;
							if(!ignoreOpen.equalsIgnoreCase("X")){
								stmtPO = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ? AND RECEIVE_FLAG = '0'");
								stmtPO.setString(1, customer);
								stmtPO.setString(2, rs.getString("ITEM_ID"));
								stmtPO.setString(3, parData.getParArea());
								ResultSet rsPO = stmtPO.executeQuery();
								
								while(rsPO.next()){						
									//Check if stock UOM is in order UOM
									if(rs.getString("UOM").equalsIgnoreCase(rsItem.getString("VENDOR_UOM")))
										onOrder = onOrder + (Integer.parseInt(rsPO.getString("QUANTITY")) - Integer.parseInt(rsPO.getString("RECEIVE_QUANTITY")));
									else
									//convert order UOM to Stock UOM
										onOrder = onOrder + ((Integer.parseInt(rsPO.getString("QUANTITY"))*Integer.parseInt(rsItem.getString("MULTIPLIER"))) - (Integer.parseInt(rsPO.getString("RECEIVE_QUANTITY"))*Integer.parseInt(rsItem.getString("MULTIPLIER"))));
								}
								stmtPO.close();	
							}
//							stmtPO = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ? AND RECEIVE_FLAG = '0'");
//							stmtPO.setString(1, customer);
//							stmtPO.setString(2, rs.getString("ITEM_ID"));
//							stmtPO.setString(3, parData.getParArea());
//							ResultSet rsPO = stmtPO.executeQuery();
//								
//							while(rsPO.next()){						
//								//Check if stock UOM is in order UOM
//								if(rs.getString("UOM").equalsIgnoreCase(rsItem.getString("VENDOR_UOM")))
//									onOrder = onOrder + Integer.parseInt(rsPO.getString("QUANTITY"));
//								else
//								//convert order UOM to Stock UOM
//									onOrder = onOrder + Integer.parseInt(rsPO.getString("QUANTITY"))*Integer.parseInt(rsItem.getString("MULTIPLIER"));
//							}
//							stmtPO.close();							
							
							if(orderLevel.equalsIgnoreCase("critical")){							
								int orderQty = Integer.parseInt(rs.getString("CRITICAL_LEVEL")) - (Integer.parseInt(rs.getString("ON_HAND_QUANTITY"))+ onOrder);
								if(orderQty >= 0 && Integer.parseInt(rs.getString("PAR_LEVEL")) > 0){
									vendorStmt = conn.prepareStatement("SELECT VENDOR_NAME FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
									vendorStmt.setString(1, customer);
									vendorStmt.setString(2, rs.getString("VENDOR_GUID"));
									ResultSet rsVendor = vendorStmt.executeQuery();
									rsVendor.next();
									String vendor = rsVendor.getString("VENDOR_NAME");
								
									String vendorData = (String) vendorMap.get(vendor);
							
									if(vendorData == null){
										ParScanPOBean poData = new ParScanPOBean();
										vendorMap.put(vendor, vendor);
								
										poData.setShipTo(parData.getShipTo());
										poData.setVendor(vendor);
								
										poList.add(poData);
									}	
	
									vendorStmt.close();								
								}											
							}else{							
								int orderQty = Integer.parseInt(rs.getString("PAR_LEVEL")) - (Integer.parseInt(rs.getString("ON_HAND_QUANTITY"))+ onOrder);
								if(orderQty > 0){
									vendorStmt = conn.prepareStatement("SELECT VENDOR_NAME FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");								
									vendorStmt.setString(1, customer);
									vendorStmt.setString(2, rs.getString("VENDOR_GUID"));
									ResultSet rsVendor = vendorStmt.executeQuery();
									rsVendor.next();
									String vendor = rsVendor.getString("VENDOR_NAME");
								
									String vendorData = (String) vendorMap.get(vendor);
							
									if(vendorData == null){
										ParScanPOBean poData = new ParScanPOBean();
										vendorMap.put(vendor, vendor);
								
										poData.setShipTo(parData.getShipTo());
										poData.setVendor(vendor);
								
										poList.add(poData);
									}	
	
									vendorStmt.close();
								}					
							}					
						}						
						stmtItem.close();
					}							
				}
				stmt.close();					
			}
			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getPONumberData: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	

		return poList;
	}
	
	/**
	 * Business Method.
	 */	
	public void updateOrderQuantity(String customer, String poNumber, ParScanPOBean data)throws ParScanResidentsEJBException{
		PreparedStatement stmt;	
		Connection conn = null;
		
		try{
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
		
			stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET COST = ? WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ITEM_GUID = ?");
			stmt.setString(1, data.getOrderCost());
			stmt.setString(2, customer);
			stmt.setString(3, poNumber);
			stmt.setString(4, data.getProductGUID());
			stmt.executeUpdate();
			stmt.close();

			stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET QUANTITY = ? WHERE CUSTOMER = ? AND PO_NUMBER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
			stmt.setString(1, data.getOrderQuantity());
			stmt.setString(2, customer);
			stmt.setString(3, poNumber);
			stmt.setString(4, data.getProductGUID());
			stmt.setString(5, data.getParArea());
			stmt.executeUpdate();
			stmt.close();

			stmt = conn.prepareStatement("UPDATE PARSCAN_ITEM SET CURRENT_COST = ? WHERE CUSTOMER = ? AND ID = ?");
			stmt.setString(1, data.getOrderCost());
			stmt.setString(2, customer);
			stmt.setString(3, data.getProductGUID());
			stmt.executeUpdate();
			stmt.close();
		
			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during updateOrderQuantity: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
		
	/**
	 * Business Method.
	 */		
	public void createOrders(String customer, String orderLevel, List poList, List parAreaList, String orderType, boolean dssi, String onlyToday, String ignoreOrder)throws ParScanResidentsEJBException{
		List orderList = new ArrayList();

		PreparedStatement stmt;
		PreparedStatement stmtPO;
		PreparedStatement stmtItem;
		PreparedStatement stmtVendor;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			java.util.Date stockDate = new java.util.Date();
			stockDate = dateFormat.parse(dateFormat.format(stockDate));
			Timestamp todayStamp = new Timestamp(stockDate.getTime());
						
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();

			for(Iterator poItor = poList.iterator();poItor.hasNext();){
				ParScanPOBean poData = (ParScanPOBean) poItor.next();

				for(Iterator itor = parAreaList.iterator();itor.hasNext();){
					ParScanPOBean data = (ParScanPOBean) itor.next();

					if(poData.getShipTo().equalsIgnoreCase(data.getShipTo())){				
						stmtVendor = conn.prepareStatement("SELECT ID FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
						stmtVendor.setString(1, customer);
						stmtVendor.setString(2, poData.getVendor());
						ResultSet rsVendor = stmtVendor.executeQuery();
						rsVendor.next();
						
						if(onlyToday.equalsIgnoreCase("X")){
							stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND VENDOR_GUID = ? AND CHANGE_TIMESTAMP >= ? ORDER BY ITEM_ID");						
						}else{
							stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND VENDOR_GUID = ? ORDER BY ITEM_ID");						
						}						
						
											
						stmt.setString(1, customer);
						stmt.setString(2, data.getParArea());
						stmt.setString(3, rsVendor.getString("ID"));
						if(onlyToday.equalsIgnoreCase("X"))
							stmt.setTimestamp(4, todayStamp);
						
						ResultSet rs = stmt.executeQuery();
						
						while(rs.next()){
							stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
							stmtItem.setString(1, customer);
							stmtItem.setString(2, rs.getString("ITEM_GUID"));
							ResultSet rsItem = stmtItem.executeQuery();
							rsItem.next();
					
							if(rsItem.getString("KIT_FLAG") == null){
								double onOrder = 0;
								
								if(!ignoreOrder.equalsIgnoreCase("X")){
									stmtPO = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ? AND RECEIVE_FLAG = ?");
									stmtPO.setString(1, customer);
									stmtPO.setString(2, rs.getString("ITEM_ID"));
									stmtPO.setString(3, data.getParArea());
									stmtPO.setString(4,"0");
									ResultSet rsPO = stmtPO.executeQuery();
							
									while(rsPO.next()){						
										//Check if stock UOM is in order UOM
										if(rs.getString("UOM").equalsIgnoreCase(rsItem.getString("VENDOR_UOM")))
											onOrder = onOrder + (Double.parseDouble(rsPO.getString("QUANTITY")) - Double.parseDouble(rsPO.getString("RECEIVE_QUANTITY")));
										else
										//convert order UOM to Stock UOM
											onOrder = onOrder + ((Double.parseDouble(rsPO.getString("QUANTITY"))*Double.parseDouble(rsItem.getString("MULTIPLIER"))) - (Double.parseDouble(rsPO.getString("RECEIVE_QUANTITY"))*Double.parseDouble(rsItem.getString("MULTIPLIER"))));
									}
									stmtPO.close();								
								}	
								
								if(orderLevel.equalsIgnoreCase("critical")){										
									int orderQty = (int)Math.ceil(Double.parseDouble(rs.getString("CRITICAL_LEVEL")) - (Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) + onOrder));

									if(orderQty >= 0 && Double.parseDouble(rs.getString("PAR_LEVEL")) > 0){
										//Convert to vendor UOM
										if(rsItem.getString("VENDOR_UOM").equalsIgnoreCase(rs.getString("UOM"))){
											orderQty = (int)Math.ceil(Double.parseDouble(rs.getString("PAR_LEVEL")) - Double.parseDouble(rs.getString("ON_HAND_QUANTITY")));										
										}else{
											double multiplier = Double.parseDouble(rsItem.getString("MULTIPLIER"));
											orderQty = (int) Math.ceil(Double.parseDouble(rs.getString("PAR_LEVEL"))/multiplier - (Double.parseDouble(rs.getString("ON_HAND_QUANTITY"))/multiplier + (double)onOrder));
										}									
										
										if(orderQty > 0){
											ParScanPOBean orderData = new ParScanPOBean();

											orderData.setPoNumber(poData.getPoNumber());
											orderData.setParArea(data.getParArea());
											orderData.setProductGUID(rs.getString("ITEM_GUID"));
											orderData.setProductID(rsItem.getString("ITEM_ID"));
											orderData.setVendor(poData.getVendor());
											orderData.setOrderQuantity(Integer.toString((int)orderQty));
											orderData.setOrderCost(rsItem.getString("CURRENT_COST"));
											orderData.setShipTo(poData.getShipTo());
											orderData.setUOM(rsItem.getString("VENDOR_UOM"));
											orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
											orderData.setReceiveQuantity("0");
								
											orderList.add(orderData);										
										}
									}
									stmtItem.close();						
								}else{
									int orderQty = (int)Math.ceil(Double.parseDouble(rs.getString("PAR_LEVEL")) - (Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) + onOrder));
									if(orderQty > 0){
										//Convert to vendor UOM
										if(!rsItem.getString("VENDOR_UOM").equalsIgnoreCase(rs.getString("UOM"))){										
											double multiplier = Double.parseDouble(rsItem.getString("MULTIPLIER"));
											orderQty = (int) Math.ceil(Double.parseDouble(rs.getString("PAR_LEVEL"))/multiplier - (Double.parseDouble(rs.getString("ON_HAND_QUANTITY"))/multiplier + (double)onOrder));
										}										
										ParScanPOBean orderData = new ParScanPOBean();
																
										orderData.setPoNumber(poData.getPoNumber());
										orderData.setParArea(data.getParArea());
										orderData.setProductGUID(rs.getString("ITEM_GUID"));
										orderData.setVendor(poData.getVendor());
										orderData.setOrderQuantity(Integer.toString(orderQty));
										orderData.setOrderCost(rsItem.getString("CURRENT_COST"));
										orderData.setProductID(rsItem.getString("ITEM_ID"));
										orderData.setShipTo(poData.getShipTo());
										orderData.setUOM(rsItem.getString("VENDOR_UOM"));
										orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));	
										orderData.setReceiveQuantity("0");								
								
										orderList.add(orderData);									
									}
									stmtItem.close();					
								}							
							}																		
						}
												
						stmtVendor.close();
						stmt.close();
					}
				}
				
				//orderList = consolidateOrder(orderList);
				createPO(orderList, customer, orderType, dssi);
				orderList.clear();
			}

			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createOrders: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
	
	private List consolidateOrder(List orderList)throws ParScanResidentsEJBException{
		try{
			Map tmpMap = new HashMap();
			
			Iterator itor = orderList.iterator();
			while(itor.hasNext()){
				ParScanPOBean prodData = (ParScanPOBean) itor.next();				
				ParScanPOBean data = (ParScanPOBean) tmpMap.get(prodData.getProductID());
				
				if(data == null){
					tmpMap.put(prodData.getProductID(), prodData);                        
				}else{
					//add quantities
					data.setOrderQuantity(Double.toString(Double.parseDouble(data.getOrderQuantity()) + Double.parseDouble(prodData.getOrderQuantity())));
					data.setReceiveQuantity(Double.toString(Double.parseDouble(data.getReceiveQuantity()) + Double.parseDouble(prodData.getReceiveQuantity())));				
				}						
			}
			
			List masterList = new ArrayList(tmpMap.values());
		
			return masterList;
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during consolidateOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}				
	}	
	
	/**
	 * Business Method.
	 */		
	public void createRequistions(String customer, String orderLevel, List parAreaList, String onlyToday, String ignoreOrder)throws ParScanResidentsEJBException{
		List orderList = new ArrayList();

		PreparedStatement stmt;
		PreparedStatement stmtItem;
		PreparedStatement stmtPO;
		PreparedStatement stmtVendor;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			java.util.Date stockDate = new java.util.Date();
			stockDate = dateFormat.parse(dateFormat.format(stockDate));
			Timestamp todayStamp = new Timestamp(stockDate.getTime());
						
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			String shipto = "";
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");
			
			for(Iterator itor = parAreaList.iterator();itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
				shipto = data.getShipTo(); 
				
				if(onlyToday.equalsIgnoreCase("X")){
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = '" + customer + "' AND CHANGE_TIMESTAMP >= ?");
					stmt.setString(1,data.getParArea().toString());
					stmt.setTimestamp(2, todayStamp);						
				}else{
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = '" + customer + "'");
					stmt.setString(1,data.getParArea().toString());						
				}
				ResultSet rs = stmt.executeQuery();
				
				while(rs.next()){	
					int onOrder = 0;
					if(!ignoreOrder.equalsIgnoreCase("X")){
						stmtPO = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ? AND RECEIVE_FLAG = ?");
						stmtPO.setString(1, customer);
						stmtPO.setString(2, rs.getString("ITEM_ID"));
						stmtPO.setString(3, data.getParArea());
						stmtPO.setString(4,"0");
						ResultSet rsPO = stmtPO.executeQuery();
							
						while(rsPO.next()){
							onOrder = onOrder + Integer.parseInt(rsPO.getString("QUANTITY"));
						}
						stmtPO.close();					
					}

					if(orderLevel.equalsIgnoreCase("critical")){
						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmtItem.setString(1, customer);
						stmtItem.setString(2, rs.getString("ITEM_GUID"));
						ResultSet rsItem = stmtItem.executeQuery();
						
						rsItem.next();								
					
						int orderQty = Integer.parseInt(rs.getString("CRITICAL_LEVEL")) - Integer.parseInt(rs.getString("ON_HAND_QUANTITY")) + onOrder;

						if(orderQty >= 0 && Integer.parseInt(rs.getString("PAR_LEVEL")) > 0){
							String vendor = "";
							int reorderUOP = 0;
							int reorderUOI = 0;
							int criticalLevel = Integer.parseInt(rs.getString("CRITICAL_LEVEL"));
							int parLevel = Integer.parseInt(rs.getString("PAR_LEVEL"));
							int onHand = Integer.parseInt(rs.getString("ON_HAND_QUANTITY"));							
							
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
							stmtVendor.setString(1, customer);
							stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
							ResultSet rsVendor = stmtVendor.executeQuery();
							
							if(rsVendor.next())
								vendor = rsVendor.getString("VENDOR_NAME");
							stmtVendor.close();			
							
							int multiplier = Integer.parseInt(rsItem.getString("MULTIPLIER"));
							if(rsItem.getString("VENDOR_UOM").equalsIgnoreCase(rs.getString("UOM"))){
								//In Vendor UOM
								reorderUOP = parLevel - onHand;
								criticalLevel = criticalLevel * multiplier;
								parLevel = parLevel * multiplier;
								onHand = onHand * multiplier;	
								reorderUOI = parLevel - onHand + (onOrder*multiplier);									
							}else{
								//In Issue UOM
								reorderUOI = parLevel - onHand;									
								reorderUOP = (int) Math.ceil((double)parLevel/multiplier - onHand/multiplier + onOrder/multiplier);
							}									

							ParScanStockBean orderData = new ParScanStockBean();

							orderData.setItemGuid(rs.getString("ITEM_GUID"));
							orderData.setParAreaGuid(data.getParArea());
							orderData.setCustomer(data.getShipTo());
							orderData.setItemID(rsItem.getString("ITEM_ID"));							
							orderData.setItemDescription(rsItem.getString("DESCRIPTION"));
							orderData.setVendor(vendor);
							orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
							orderData.setCasePackaging(rsItem.getString("CASE_PACKAGING"));
							orderData.setVendorUOM(rsItem.getString("VENDOR_UOM"));
							orderData.setIssueUOM(rsItem.getString("BILL_UOM"));
							if(rsItem.getString("CURRENT_COST") != null){
								//orderData.setCost(Double.toString((Double.parseDouble(rsItem.getString("CURRENT_COST"))/multiplier)*reorderUOI));
								orderData.setCost(outNumberFormat.format(Double.parseDouble(rsItem.getString("CURRENT_COST"))/multiplier));
								orderData.setTotalCost("0");
							}								
							else{
								orderData.setCost("0");
								orderData.setTotalCost("0");	
							}								
							orderData.setCriticalLevel(Integer.toString(criticalLevel));
							orderData.setParLevel(Integer.toString(parLevel));
							orderData.setOnHandQuantity(Integer.toString(onHand));
							orderData.setOnOrderQuantity(Integer.toString(onOrder));
							orderData.setReorderQtyUOI(Integer.toString(reorderUOI));
							orderData.setReorderQtyUOP(Integer.toString(reorderUOP));
							orderData.setAlternateBarcode("");
							if(rsItem.getString("ALTERNATE_BARCODE") != null)
								orderData.setAlternateBarcode(rsItem.getString("ALTERNATE_BARCODE"));
														
							orderList.add(orderData);																
						}					
						stmtItem.close();
					}else{
						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmtItem.setString(1, customer);
						stmtItem.setString(2, rs.getString("ITEM_GUID"));
						ResultSet rsItem = stmtItem.executeQuery();
						
						rsItem.next();
					
						int orderQty = Integer.parseInt(rs.getString("PAR_LEVEL")) - Integer.parseInt(rs.getString("ON_HAND_QUANTITY")) + onOrder;
						if(orderQty > 0){
							String vendor = "";
							int reorderUOP = 0;
							int reorderUOI = 0;
							
							int criticalLevel = Integer.parseInt(rs.getString("CRITICAL_LEVEL"));
							int parLevel = Integer.parseInt(rs.getString("PAR_LEVEL"));
							int onHand = Integer.parseInt(rs.getString("ON_HAND_QUANTITY"));
							
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
							stmtVendor.setString(1, customer);
							stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
							ResultSet rsVendor = stmtVendor.executeQuery();
							
							if(rsVendor.next())
								vendor = rsVendor.getString("VENDOR_NAME");
							stmtVendor.close();			
							
							int multiplier = Integer.parseInt(rsItem.getString("MULTIPLIER"));
							if(rsItem.getString("VENDOR_UOM").equalsIgnoreCase(rs.getString("UOM"))){
								//In Vendor UOM
								reorderUOP = parLevel - onHand;
								criticalLevel = criticalLevel * multiplier;
								parLevel = parLevel * multiplier;
								onHand = onHand * multiplier;	
								reorderUOI = parLevel - onHand;									
							}else{
								//In Issue UOM
								reorderUOI = parLevel - onHand;								
								reorderUOP = (int) Math.ceil((double)parLevel/multiplier - onHand/multiplier);
							}									
							
							ParScanStockBean orderData = new ParScanStockBean();

							orderData.setItemGuid(rs.getString("ITEM_GUID"));
							orderData.setParAreaGuid(data.getParArea());
							orderData.setCustomer(data.getShipTo());
							orderData.setItemID(rsItem.getString("ITEM_ID"));							
							orderData.setItemDescription(rsItem.getString("DESCRIPTION"));
							orderData.setVendor(vendor);
							orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
							orderData.setCasePackaging(rsItem.getString("CASE_PACKAGING"));
							orderData.setVendorUOM(rsItem.getString("VENDOR_UOM"));
							orderData.setIssueUOM(rsItem.getString("BILL_UOM"));
							if(rsItem.getString("CURRENT_COST") != null){
								orderData.setCost(outNumberFormat.format(Double.parseDouble(rsItem.getString("CURRENT_COST"))/multiplier));
								orderData.setTotalCost("0");
							}								
							else{
								orderData.setCost("0");
								orderData.setTotalCost("0");	
							}															
							orderData.setCriticalLevel(Integer.toString(criticalLevel));
							orderData.setParLevel(Integer.toString(parLevel));
							orderData.setOnHandQuantity(Integer.toString(onHand));
							orderData.setOnOrderQuantity(Integer.toString(onOrder));
							orderData.setReorderQtyUOI(Integer.toString(reorderUOI));
							orderData.setReorderQtyUOP(Integer.toString(reorderUOP));
							orderData.setAlternateBarcode("");
							if(rsItem.getString("ALTERNATE_BARCODE") != null)
								orderData.setAlternateBarcode(rsItem.getString("ALTERNATE_BARCODE"));
							
							orderList.add(orderData);									
						}					
						stmtItem.close();
					}							
				}				
				stmt.close();
				
				//CREATE REQ COPY, REQ FILE AND SEND TO FTP
				String fileName = "";
				DateFormat dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
				String sysdate = dateFormat2.format(date);
				sysdate = sysdate.replaceAll("/","");
				sysdate = sysdate.replaceAll(" ","_");
				sysdate = sysdate.replaceAll(":","");			
				fileName= data.getParArea()+"_" + sysdate + ".txt";	
				
				createParScanReqCopy(customer, shipto, orderList);
				InputStream iStream = new ParScanRequisitionInterfaces().createRequisition(customer, orderList);
				uploadReqFiletoFTP("ejgh_ftp", "ejghFTP2014", fileName, iStream);
				
				orderList.clear();				 
			}				
			
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createRequisitions: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}	
	
	private void createScanParScanReqCopy(ParScanStockBean[] orderData)throws ParScanResidentsEJBException{
		Connection conn = null;
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		PreparedStatement stmtVendor;
		List parAreaList = new ArrayList();
		List poList = new ArrayList();
		List orderList = new ArrayList();
		Map tmpMap = new HashMap();
		Map vendorMap = new HashMap();
		Map shipMap = new HashMap();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			conn = openConnection();
			java.util.Date date = new java.util.Date();
			String strDate = dateFormat.format(date);
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");
			
			//Get Par Area/Ship-to list
			for(int i = 0; i < orderData.length; i++){
				if(isInteger(orderData[i].getOrderQuantity())){
					ParScanStockBean data = (ParScanStockBean) tmpMap.get(orderData[i].getParAreaGuid());
				
					if(data == null){
						tmpMap.put(orderData[i].getParAreaGuid(), orderData[i]);
					
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PARAREA WHERE CUSTOMER = ? AND PAR_AREA = ?");
						stmt.setString(1,orderData[i].getCustomer());
						stmt.setString(2,orderData[i].getParAreaGuid());
						ResultSet rs = stmt.executeQuery();
	
						while(rs.next()){
							ParScanPOBean parData = new ParScanPOBean();
							parData.setParArea(orderData[i].getParAreaGuid());
							parData.setShipTo(rs.getString("SHIP_TO"));
							parData.setCustomer(orderData[i].getCustomer());
							parAreaList.add(parData);
						}	
					}				
				}						
			}	
			
//			//Get PO List and create Order
//			for(Iterator itor = parAreaList.iterator();itor.hasNext();){
//				ParScanPOBean parData = (ParScanPOBean) itor.next();
//				
//				for(int i = 0; i < orderData.length; i++){
//					if(isInteger(orderData[i].getOrderQuantity())){
//						if(orderData[i].getParAreaGuid().equalsIgnoreCase(parData.getParArea())){			
//							String shipToData = (String) shipMap.get(parData.getShipTo());
//					
//							if(shipToData == null){
//								shipMap.put(parData.getShipTo(), parData.getShipTo());
//								vendorMap.clear();
//							}				
//						
//							stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = ? AND ITEM_GUID = ?");														
//							stmt.setString(1, parData.getParArea());
//							stmt.setString(2,parData.getCustomer());
//							stmt.setString(3,orderData[i].getItemGuid());						
//							ResultSet rs = stmt.executeQuery();
//					
//							while(rs.next()){
//								stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
//								stmtItem.setString(1, parData.getCustomer());
//								stmtItem.setString(2, orderData[i].getItemGuid());
//								ResultSet rsItem = stmtItem.executeQuery();
//								rsItem.next();
//							
//								if(rsItem.getString("KIT_FLAG") == null){
//									if(rs.getString("VENDOR_GUID") != null){
//										stmtVendor = conn.prepareStatement("SELECT VENDOR_NAME FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
//										stmtVendor.setString(1, orderData[i].getCustomer());
//										stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
//										ResultSet rsVendor = stmtVendor.executeQuery();
//										rsVendor.next();
//										String vendor = rsVendor.getString("VENDOR_NAME");							
//										String vendorData = (String) vendorMap.get(vendor);
//						
//										if(vendorData == null){
//											ParScanPOBean poData = new ParScanPOBean();
//											vendorMap.put(vendor, vendor);
//							
//											poData.setCustomer(parData.getCustomer());
//											poData.setShipTo(parData.getShipTo());
//											poData.setVendor(vendor);
//											if(vendor.equalsIgnoreCase("Medline Industries"))
//												poData.setPoNumber("Medl"+parData.getShipTo().substring(3)+"-"+strDate.substring(0,10).replaceAll("/",""));
//											else{
//												if(vendor.length() > 4)										
//													poData.setPoNumber(vendor.substring(0,3)+parData.getShipTo().substring(3)+"-"+strDate.substring(0,10).replaceAll("/",""));
//												else{
//													poData.setPoNumber(vendor+parData.getShipTo().substring(3)+"-"+strDate.substring(0,10).replaceAll("/",""));
//												}												
//											}
//																													
//											poList.add(poData);
//										}	
//
//										stmtVendor.close();	
//									}										
//								}						
//								stmtItem.close();							
//							}
//							stmt.close();	
//						}					
//					}
//				}				
//			}
			
			//Create Orders
//			for(Iterator poItor = poList.iterator();poItor.hasNext();){
//				ParScanPOBean poData = (ParScanPOBean) poItor.next();

				for(Iterator itor = parAreaList.iterator();itor.hasNext();){
					ParScanPOBean data = (ParScanPOBean) itor.next();

					for(int i = 0; i < orderData.length; i++){
						if(isInteger(orderData[i].getOrderQuantity())){
							if(orderData[i].getParAreaGuid().equalsIgnoreCase(data.getParArea())){				
//								stmtVendor = conn.prepareStatement("SELECT ID FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
//								stmtVendor.setString(1, data.getCustomer());
//								stmtVendor.setString(2, poData.getVendor());
//								ResultSet rsVendor = stmtVendor.executeQuery();
//								rsVendor.next();
							
								stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");					
								stmt.setString(1, data.getCustomer());
								stmt.setString(2, data.getParArea());
								stmt.setString(3, orderData[i].getItemGuid());
								ResultSet rs = stmt.executeQuery();
							
								while(rs.next()){
									stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
									stmtItem.setString(1, data.getCustomer());
									stmtItem.setString(2, orderData[i].getItemGuid());
									ResultSet rsItem = stmtItem.executeQuery();
									rsItem.next();
						
									if(rsItem.getString("KIT_FLAG") == null){									
										int multiplier = Integer.parseInt(rsItem.getString("MULTIPLIER"));
										//Convert to vendor UOM
										int orderQty = 0;
//										if(rsItem.getString("VENDOR_UOM").equalsIgnoreCase(orderData[i].getScannedUOM())){
											orderQty = Integer.parseInt(orderData[i].getOrderQuantity());										
//										}else{
//											double multiplier = Double.parseDouble(rsItem.getString("MULTIPLIER"));
//											orderQty = (int) Math.ceil(Double.parseDouble(orderData[i].getOrderQuantity())/multiplier);
//										}									

										ParScanPOBean newOrderData = new ParScanPOBean();	
										newOrderData.setPoNumber(data.getParArea()+"_"+strDate.substring(0,10));
										newOrderData.setParArea(data.getParArea());
										newOrderData.setProductGUID(rs.getString("ITEM_GUID"));
										newOrderData.setProductID(rsItem.getString("ITEM_ID"));
										newOrderData.setDescription(rsItem.getString("DESCRIPTION"));
										newOrderData.setVendor(rsItem.getString("VENDOR_ID"));
										newOrderData.setOrderQuantity(Integer.toString((int)orderQty));
										if(rsItem.getString("CURRENT_COST") != null){
											//orderData.setCost(Double.toString((Double.parseDouble(rsItem.getString("CURRENT_COST"))/multiplier)*reorderUOI));
											newOrderData.setOrderCost(outNumberFormat.format(Double.parseDouble(rsItem.getString("CURRENT_COST"))/multiplier));
										}								
										else{
											newOrderData.setOrderCost("0");	
										}											
										newOrderData.setShipTo(data.getShipTo());
										newOrderData.setUOM(rsItem.getString("BILL_UOM"));
										newOrderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));									
										orderList.add(newOrderData);									
																		
										stmtItem.close();																		
									}																		
								}
													
//								stmtVendor.close();
								stmt.close();
							}						
						}
					}
					
					createScanReq(orderList, data.getCustomer());
					//CREATE REQ COPY, REQ FILE AND SEND TO FTP					
					String fileName = "";
					DateFormat dateFormat2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					date = new Date();
					String sysdate = dateFormat2.format(date);
					sysdate = sysdate.replaceAll("/","");
					sysdate = sysdate.replaceAll(" ","_");
					sysdate = sysdate.replaceAll(":","");			
					fileName= data.getParArea()+"_" + sysdate + ".txt";	
				
					List reqList = new ArrayList();
				
					for(Iterator orderItor = orderList.iterator();orderItor.hasNext();){
						ParScanPOBean orderdata = (ParScanPOBean) orderItor.next();
						
						ParScanStockBean reqData = new ParScanStockBean();
						
						reqData.setParAreaGuid(orderdata.getParArea());
						reqData.setItemID(orderdata.getProductID());
						reqData.setIssueUOM(orderdata.getUOM());
						reqData.setItemDescription(orderdata.getDescription());
						reqData.setReorderQtyUOI(orderdata.getOrderQuantity());
						
						reqList.add(reqData);
					}
					
					
					InputStream iStream = new ParScanRequisitionInterfaces().createRequisition(data.getCustomer(), reqList);
					uploadReqFiletoFTP("ejgh_ftp", "ejghFTP2014", fileName, iStream);															
					orderList.clear();
				}

			//}			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createScanOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
	
	private void createScanReq(List orderList, String customer)throws ParScanResidentsEJBException{
		PreparedStatement stmtPO;
		PreparedStatement stmtVendor;
		PreparedStatement stmtStock;			
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			java.util.Date utilDate = new java.util.Date();
			java.util.Date date = new java.util.Date();
			String strDate = dateFormat.format(date);
			Timestamp sqlDate = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());
			
			for(Iterator itor = orderList.iterator();itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
//				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
//				stmtVendor.setString(1, customer);
//				stmtVendor.setString(2, data.getVendor());
//				ResultSet rsVendor = stmtVendor.executeQuery();
//				rsVendor.next();
				
				stmtPO = conn.prepareStatement("INSERT INTO PARSCAN_ORDER (ID, CUSTOMER, PO_NUMBER, ITEM_GUID, VENDOR_GUID, QUANTITY, COST, CREATE_DATE, SHIP_TO, PARSCAN_USER, ITEM_ID, PAR_AREA, RECEIVE_FLAG, SENT_MEDLINE, RECEIVE_QUANTITY, UOM) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();									
				stmtPO.setString(1, uid.toString());
				stmtPO.setString(2, customer);
				stmtPO.setString(3, data.getPoNumber());
				stmtPO.setString(4,data.getProductGUID());
				stmtPO.setString(5, data.getVendor());
				stmtPO.setString(6,data.getOrderQuantity());
				stmtPO.setString(7, data.getOrderCost());
				stmtPO.setTimestamp(8, sqlDate);
				stmtPO.setString(9, data.getShipTo());
				stmtPO.setString(10, user);
				stmtPO.setString(11, data.getProductID());
				stmtPO.setString(12, data.getParArea());
				stmtPO.setString(13, "0");
				stmtPO.setString(14, "0");
				stmtPO.setString(15,"0");	
				stmtPO.setString(16,data.getUOM());			
				stmtPO.executeUpdate();
				stmtPO.close();	
//				stmtVendor.close();			
			}			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createScanReq: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	private void createParScanReqCopy(String customer, String shipto, List orderList)throws ParScanResidentsEJBException{
		PreparedStatement stmtPO;
		PreparedStatement stmtVendor;
		PreparedStatement stmtStock;			
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			java.util.Date utilDate = new java.util.Date();
			java.util.Date date = new java.util.Date();
			String strDate = dateFormat.format(date);
			Timestamp sqlDate = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());
			
			for(Iterator itor = orderList.iterator();itor.hasNext();){
				ParScanStockBean data = (ParScanStockBean) itor.next();
				
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, data.getVendor());
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				
				stmtPO = conn.prepareStatement("INSERT INTO PARSCAN_ORDER (ID, CUSTOMER, PO_NUMBER, ITEM_GUID, VENDOR_GUID, QUANTITY, COST, CREATE_DATE, SHIP_TO, PARSCAN_USER, ITEM_ID, PAR_AREA, RECEIVE_FLAG, SENT_MEDLINE, RECEIVE_QUANTITY, UOM) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();									
				stmtPO.setString(1, uid.toString());
				stmtPO.setString(2, customer);
				stmtPO.setString(3, data.getParAreaGuid()+"_"+strDate.substring(0,10));
				stmtPO.setString(4,data.getItemGuid());
				stmtPO.setString(5, rsVendor.getString("ID"));
				stmtPO.setString(6,data.getReorderQtyUOI());
				stmtPO.setString(7, data.getCost());
				stmtPO.setTimestamp(8, sqlDate);
				stmtPO.setString(9, shipto);
				stmtPO.setString(10, user);
				stmtPO.setString(11, data.getItemID());
				stmtPO.setString(12, data.getParAreaGuid());
				stmtPO.setString(13, "0");
				stmtPO.setString(14, "0");
				stmtPO.setString(15,"0");	
				stmtPO.setString(16,data.getIssueUOM());			
				stmtPO.executeUpdate();
				stmtPO.close();	
				stmtVendor.close();			
			}			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createScanParScanReqCopy: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	public String checkPONumbers(String customer, List poList) throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		String res = "";
		Connection conn = null;
		
		try{
			conn = openConnection();
			int count = 0;
			for(Iterator itor = poList.iterator(); itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
				stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getPoNumber());
				ResultSet rs = stmt.executeQuery();
				rs.next();
				if(rs.getInt("CNT") > 0){
					if(count > 1)
						res = res + ", ";
					res = res + data.getPoNumber();
					count++;	
				}
				stmt.close();			    	
			}
		
			return res;	
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during checkPONumbers: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally{			
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
	}
	
	private boolean atgUser(String customer, String user)throws ParScanResidentsEJBException{
		Connection conn = null;
		PreparedStatement stmt;
		boolean atg = false;
		
		try{
//			conn = openConnection();
//			
//			//stmt = conn.prepareStatement("SELECT COUNT(*) FROM PARSCAN_ATG_USER WHERE CUSTOMER = ? AND USER_ID = ?");
//			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ATG_USERS WHERE USER_ID = ?");
//			//stmt.setString(1, customer);
//			stmt.setString(1, user.toUpperCase());
//			
//			ResultSet rs = stmt.executeQuery();
//			rs.next();
//			if(rs.getInt("CNT") > 0){
//				atg = true;
//			}
//			
//			return atg;
			return true;
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during atgUser: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}
	
	private void createPO(List orderList, String customer, String orderType, boolean dssi)throws ParScanResidentsEJBException{
		PreparedStatement stmtPO;
		PreparedStatement stmtVendor;
		PreparedStatement stmtStock;			
		Connection conn = null;
		boolean medlinePO = false;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			java.util.Date utilDate = new java.util.Date();
			Timestamp sqlDate = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());

			for(Iterator itor = orderList.iterator();itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
				if(data.getVendor().toUpperCase().indexOf("MEDLINE") != -1)
					medlinePO = true;
				
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, data.getVendor());
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				
				stmtPO = conn.prepareStatement("INSERT INTO PARSCAN_ORDER (ID, CUSTOMER, PO_NUMBER, ITEM_GUID, VENDOR_GUID, QUANTITY, COST, CREATE_DATE, SHIP_TO, PARSCAN_USER, ITEM_ID, PAR_AREA, RECEIVE_FLAG, SENT_MEDLINE, RECEIVE_QUANTITY, UOM) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();									
				stmtPO.setString(1, uid.toString());
				stmtPO.setString(2, customer);
				stmtPO.setString(3, data.getPoNumber());
				stmtPO.setString(4,data.getProductGUID());
				stmtPO.setString(5, rsVendor.getString("ID"));
				stmtPO.setString(6,data.getOrderQuantity());
				stmtPO.setString(7, data.getOrderCost());
				stmtPO.setTimestamp(8, sqlDate);
				stmtPO.setString(9, data.getShipTo());
				stmtPO.setString(10, user);
				stmtPO.setString(11, data.getProductID());
				stmtPO.setString(12, data.getParArea());
				stmtPO.setString(13, "0");
				stmtPO.setString(14, "0");
				stmtPO.setString(15,"0");	
				stmtPO.setString(16,data.getUOM());			
				stmtPO.executeUpdate();
				stmtPO.close();	
				stmtVendor.close();

				stmtStock = conn.prepareStatement("UPDATE PARSCAN_STOCK SET SCANNED = 'N' WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_ID = ?");
				stmtStock.setString(1, customer);
				stmtStock.setString(2, data.getParArea());
				stmtStock.setString(3, data.getProductID());
				stmtStock.executeUpdate();
				stmtStock.close();				
			}
		
			if(medlinePO && !dssi){		
				if(atgUser(customer, myContext.getCallerPrincipal().getName()))		
					createATGOrder(consolidateOrder(orderList), customer, orderType, myContext.getCallerPrincipal().getName());
				else
					createMedlineOrder(consolidateOrder(orderList), customer, orderType);
			}				
		
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createPO: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Business Method.
	 */		
	public void resendMedlinePO(String customer, List poList)throws ParScanResidentsEJBException{
		List orderList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;

		Connection conn = null;
		
		try{
			conn = openConnection();
			
			for(Iterator itor = poList.iterator();itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
			
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getPoNumber());
				ResultSet rs = stmt.executeQuery();
				while(rs.next()){
					ParScanPOBean orderData = new ParScanPOBean();
					orderData.setPoNumber(data.getPoNumber());
					orderData.setOrderQuantity(rs.getString("QUANTITY"));
					orderData.setShipTo(data.getShipTo());
					
					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmtItem.setString(1, customer);
					stmtItem.setString(2, rs.getString("ITEM_GUID"));
					ResultSet rsItem = stmtItem.executeQuery();
					
					if(rsItem.next()){
						orderData.setUOM(rsItem.getString("VENDOR_UOM"));
						orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));								
						orderList.add(orderData);
					}
					stmtItem.close();					
				}		
				stmt.close();
				
				String orderType = "TA";
				if(checkConsignment(customer).equalsIgnoreCase("X"))
					orderType = "KB";
				
				if(atgUser(customer, myContext.getCallerPrincipal().getName()))		
					createATGOrder(orderList, customer, orderType, myContext.getCallerPrincipal().getName());
				else
					createMedlineOrder(orderList, customer, orderType);				
				
				orderList.clear();
			}			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during resendMedlinePO: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally
		{
			try
			{
				if (conn != null)
					conn.close();
			}
			catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		 	
	}

	private void createATGOrder(List orderList, String customerNumber, String orderType, String userID)throws ParScanResidentsEJBException{
		String shipTo = "";
		String poNumber = "";
		
		try{
			InitialContext ctx = new InitialContext();

			ManageOrderServiceService obj = (ManageOrderServiceService) ctx.lookup("java:comp/env/ATGOrderProxy");			
			ManageOrderService port = (ManageOrderService) obj.getLogicalPort();
                 					
			AuthenticationDetails auth = new AuthenticationDetails();
//			  auth.setUserId("serviceuser");
//			  auth.setLoginToken("connect");
			auth.setUserId(userID);
			auth.setSourceSystemId("Parscan");
                 					
			Collections.sort(orderList, new ResultComparator(false));

			LineItem[] itemList = new LineItem[orderList.size()];

			int count = 0;
			int arrayCount = 0;
			for(Iterator itor = orderList.iterator();itor.hasNext();){
				count = count + 10;
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
				shipTo = data.getShipTo();
				poNumber = data.getPoNumber();
								
				LineItem it = new LineItem();
				
				it.setLineItemNumber(Integer.toString(count));
				it.setItemNumber(data.getVendorNumber());
				it.setQuantity(Long.parseLong(removeDecimal(data.getOrderQuantity())));
				it.setUom(data.getUOM());
				it.setParArea(data.getParArea());
				
				itemList[arrayCount] = it;
				arrayCount++;
			}			
						
			ManageOrderRequest order = new ManageOrderRequest();
			order.setPoNumber(poNumber);
			order.setSoldToAccountNum(customerNumber);
			order.setShipToAccountNum(shipTo);
			if(orderType.equalsIgnoreCase("TA"))
				order.setOrderType("STANDARD");
			else	
				order.setOrderType("CONSIGNMENT");
			order.setProcessAsynchronously(true);	
			order.setLineItems(itemList);
			order.setAuthDetail(auth);
			order.setOrderCreationMode("UNSUBMIT_ORDER_BASIC_VALIDATION");
			
			ManageOrderRequest[] manageOrderList = new ManageOrderRequest[1];
			manageOrderList[0] = order;
			
			ManageOrderRequestWrapper orderWrap = new ManageOrderRequestWrapper();
			orderWrap.setOrder(manageOrderList);
			
			ManageOrder mOrder = new ManageOrder();
			mOrder.setOrders(orderWrap);
			port.manageOrder(mOrder);				
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createATGOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
	}
	
	private void createMedlineOrder(List orderList, String customerNumber, String orderType)throws ParScanResidentsEJBException{
		IConnection connection = null;
		String shipTo = "";
		String poNumber = "";
		
		try{
			connection = getCRMConnection();
			
			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "Z_SHOPPING_CART_CREATE");
		
			IFunctionsMetaData functionsMetaData = connection.getFunctionsMetaData();
			IFunction function = functionsMetaData.getFunction("Z_SHOPPING_CART_CREATE");
		
			// Create Basket
			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams = recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory = interaction.retrieveStructureFactory();
				
			IRecordSet productsIn = (IRecordSet) structureFactory.getStructure(function.getParameter("IT_ITEM").getStructure());			

			Collections.sort(orderList, new ResultComparator(false));

			int count = 0;
			for(Iterator itor = orderList.iterator();itor.hasNext();){
				count = count + 10;
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
				shipTo = data.getShipTo();
				poNumber = data.getPoNumber();
				productsIn.insertRow();
				productsIn.setInt("HANDLE", count);
				productsIn.setString("PRODUCT", data.getVendorNumber());
				productsIn.setString("QUANTITY",removeDecimal(data.getOrderQuantity()));
				productsIn.setString("UNIT",data.getUOM());
			}

			importParams.put("IT_ITEM", productsIn);
			importParams.put("IV_SOLDTO", customerNumber);
			importParams.put("IV_SHIPTO", shipTo);
			importParams.put("IV_PO_NUMBER", poNumber);
			importParams.put("IV_ORDER_TYPE", orderType);
	
			MappedRecord exportParams = (MappedRecord) interaction.execute(interactionSpec, importParams);
			IRecordSet message = (IRecordSet) exportParams.get("ET_RETURN");
			String EV_ORDERNBR = exportParams.get("EV_ORDERNBR").toString();
			String EV_ORDERGUID = exportParams.get("EV_ORDERGUID").toString();	

			if(ParScanResidentsSSBean.logger.beDebug()){
				ParScanResidentsSSBean.logger.debugT("EV_ORDERNBR: " + EV_ORDERNBR);
				ParScanResidentsSSBean.logger.debugT("EV_ORDERGUID: " + EV_ORDERGUID);			
			}
				
			while (message.next()) {
				if(ParScanResidentsSSBean.logger.beDebug())
					ParScanResidentsSSBean.logger.debugT("Return: " + message.getString("MESSAGE"));
			}
			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createMedlineOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally
		{
			try
			{
				if (connection != null)
					connection.close();
			}
			catch (ResourceException ex)
			{
			}
		}
	}	
	
	private String removeDecimal(String quantity){
		int index = quantity.indexOf(".");
		
		if(index != -1){
			quantity = quantity.substring(0,index);	
		}
		
		return quantity;
	}	
	
	/**
	 * Business Method.
	 */
	public List getOrderData(String customer, List poList, boolean consolidate)throws ParScanResidentsEJBException{
		List orderList = new ArrayList();

		PreparedStatement stmt;
		PreparedStatement stmtItem;
		PreparedStatement stmtVendor;
		PreparedStatement stmtCat;
		Connection conn = null;
		
		try{
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();

			for(Iterator poItor = poList.iterator();poItor.hasNext();){
				ParScanPOBean poData = (ParScanPOBean) poItor.next();
								
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? ORDER BY PAR_AREA ASC, ITEM_ID ASC");
				stmt.setString(1, customer);
				stmt.setString(2, poData.getPoNumber());
				
				ResultSet rs = stmt.executeQuery();
				
				while(rs.next()){	
					stmtVendor = conn.prepareStatement("SELECT ID FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
					stmtVendor.setString(1, customer);
					stmtVendor.setString(2, poData.getVendor());
					ResultSet rsVendor = stmtVendor.executeQuery();
					rsVendor.next();

					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmtItem.setString(1, customer);
					stmtItem.setString(2, rs.getString("ITEM_GUID"));
					ResultSet rsItem = stmtItem.executeQuery();
					rsItem.next();								

					ParScanPOBean orderData = new ParScanPOBean();
					
					if(rsItem.getString("CATEGORY_ID") != null){
						stmtCat = conn.prepareStatement("SELECT PRODUCT_CATEGORY FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
						stmtCat.setString(1, customer);
						stmtCat.setString(2, rsItem.getString("CATEGORY_ID"));
						ResultSet rsCat = stmtCat.executeQuery();
						rsCat.next();
						
						if(rsCat.getString("PRODUCT_CATEGORY") != null)		
							orderData.setSummary(rsCat.getString("PRODUCT_CATEGORY"));
						else
							orderData.setSummary("");
						stmtCat.close();
					}else
						orderData.setSummary("");
					
					orderData.setPoNumber(poData.getPoNumber());
					orderData.setParArea(rs.getString("PAR_AREA"));
					orderData.setProductID(rs.getString("ITEM_ID"));
					orderData.setVendor(poData.getVendor());
					orderData.setOrderQuantity(rs.getString("QUANTITY"));
					orderData.setReceiveQuantity(rs.getString("RECEIVE_QUANTITY"));
					orderData.setUOM(rs.getString("UOM"));
					orderData.setOrderCost(rs.getString("COST"));
					orderData.setDescription(rsItem.getString("DESCRIPTION"));
					orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));				
					orderList.add(orderData);
														
					stmtVendor.close();
					stmtItem.close();											
				}								
				stmt.close();
			}

			Collections.sort(orderList, new ResultComparator(false));

			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getOrderData: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}		
		
		if(consolidate)
			return consolidateOrder(orderList);
		else
			return orderList;
	}	

	/**
	 * Business Method.
	 */
	public void autoRestock(String customer, String outParArea, List parList, List orderList)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		Connection conn = null;
		
		try{
			conn = openConnection();
			
			for(Iterator stockItor = orderList.iterator();stockItor.hasNext();){
				ParScanStockBean data = (ParScanStockBean) stockItor.next();															
			
				if(!data.getParAreaGuid().equalsIgnoreCase(outParArea)){
					data.setTransferQuantity(data.getReorderQtyUOI());
					data.setCurrentUOM(data.getIssueUOM());
					
					transferServerProducts(customer, outParArea, data.getParAreaGuid(), data);
				}	
			}
			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during autoRestock: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
	

	/**
	 * Business Method.
	 */
	public void updatePricing(String customer, List itemList, Date effDate)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		Connection conn = null;
		
		try{
			Timestamp sqlDate = new Timestamp(effDate.getTime());
			conn = openConnection();

			stmt = conn.prepareStatement("UPDATE PARSCAN_RESIDENT SET PRICE = ? WHERE CUSTOMER = ? AND CHARGE_ID = ? AND CHARGE_DATE >= ?");					
			for(Iterator itor = itemList.iterator();itor.hasNext();){
				ParScanItemBean data = (ParScanItemBean) itor.next();
				if(StringUtils.isNotEmpty(data.getCurrentPrice())) {
					stmt.setString(1, data.getCurrentPrice());
					stmt.setString(2, customer);
					stmt.setString(3, data.getItemGUID());
					stmt.setTimestamp(4, sqlDate);
					stmt.executeUpdate();	
				}
			}
			stmt.close();
			conn.close();			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during updatePricing: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	/**
	 * Business Method.
	 */
	public void updateServicePricing(String customer, List serviceList, Date effDate)throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		Connection conn = null;
		
		try{
			Timestamp sqlDate = new Timestamp(effDate.getTime());
			conn = openConnection();
			
			for(Iterator itor = serviceList.iterator();itor.hasNext();){
				ParScanServiceBean data = (ParScanServiceBean) itor.next();
				
				stmt = conn.prepareStatement("UPDATE PARSCAN_RESIDENT SET PRICE = ? WHERE CUSTOMER = ? AND CHARGE_ID = ? AND CHARGE_DATE >= ?");					
				stmt.setString(1, data.getPrice());
				stmt.setString(2, customer);
				stmt.setString(3, data.getServiceGUID());
				stmt.setTimestamp(4, sqlDate);
				stmt.executeUpdate();	
				stmt.close();			
			}
			
			conn.close();			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during updateServicePricing: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	
	/**
	 * Business Method.
	 */
	public List getReorderData(String customer, String orderLevel, List parAreaList, String onlyToday, String ignoreOrder, boolean restock)throws ParScanResidentsEJBException{
		List orderList = new ArrayList();

		PreparedStatement stmt;
		PreparedStatement stmtItem;
		PreparedStatement stmtPO;
		PreparedStatement stmtVendor;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			java.util.Date stockDate = new java.util.Date();
			stockDate = dateFormat.parse(dateFormat.format(stockDate));
			Timestamp todayStamp = new Timestamp(stockDate.getTime());
						
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();

			for(Iterator itor = parAreaList.iterator();itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
				
				if(onlyToday.equalsIgnoreCase("X")){
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = '" + customer + "' AND CHANGE_TIMESTAMP >= ?");
					stmt.setString(1,data.getParArea().toString());
					stmt.setTimestamp(2, todayStamp);						
				}else{
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = '" + customer + "'");
					stmt.setString(1,data.getParArea().toString());						
				}
				ResultSet rs = stmt.executeQuery();
				
				while(rs.next()){	
					int onOrder = 0;
					if(!ignoreOrder.equalsIgnoreCase("X")){
						stmtPO = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND ITEM_ID = ? AND PAR_AREA = ? AND RECEIVE_FLAG = ?");
						stmtPO.setString(1, customer);
						stmtPO.setString(2, rs.getString("ITEM_ID"));
						stmtPO.setString(3, data.getParArea());
						stmtPO.setString(4,"0");
						ResultSet rsPO = stmtPO.executeQuery();
							
						while(rsPO.next()){
							onOrder = onOrder + Integer.parseInt(rsPO.getString("QUANTITY"));
						}
						stmtPO.close();					
					}

					if(orderLevel.equalsIgnoreCase("critical")){
						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmtItem.setString(1, customer);
						stmtItem.setString(2, rs.getString("ITEM_GUID"));
						ResultSet rsItem = stmtItem.executeQuery();
						
						rsItem.next();								
					
						int orderQty = Integer.parseInt(rs.getString("CRITICAL_LEVEL")) - Integer.parseInt(rs.getString("ON_HAND_QUANTITY")) + onOrder;

						if(orderQty >= 0 && Integer.parseInt(rs.getString("PAR_LEVEL")) > 0){
							String vendor = "";
							int reorderUOP = 0;
							int reorderUOI = 0;
							int criticalLevel = Integer.parseInt(rs.getString("CRITICAL_LEVEL"));
							int parLevel = Integer.parseInt(rs.getString("PAR_LEVEL"));
							int onHand = Integer.parseInt(rs.getString("ON_HAND_QUANTITY"));							
							
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
							stmtVendor.setString(1, customer);
							stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
							ResultSet rsVendor = stmtVendor.executeQuery();
							
							if(rsVendor.next())
								vendor = rsVendor.getString("VENDOR_NAME");
							stmtVendor.close();			
							
							int multiplier = Integer.parseInt(rsItem.getString("MULTIPLIER"));
							if(rsItem.getString("VENDOR_UOM").equalsIgnoreCase(rs.getString("UOM"))){
								//In Vendor UOM
								reorderUOP = parLevel - onHand;
								if(restock){
									criticalLevel = criticalLevel * multiplier;
									parLevel = parLevel * multiplier;
									onHand = onHand * multiplier;	
									reorderUOI = parLevel - onHand + (onOrder*multiplier);									
								}else{
									reorderUOI = (parLevel*multiplier) - (onHand*multiplier) + (onOrder*multiplier);									
								}
							}else{
								//In Issue UOM
								reorderUOI = parLevel - onHand;									
								reorderUOP = (int) Math.ceil((double)parLevel/multiplier - onHand/multiplier + onOrder/multiplier);
								if(!restock){
									criticalLevel = (int) Math.ceil((double)criticalLevel / multiplier);
									parLevel = (int) Math.ceil((double)parLevel / multiplier);
									onHand = (int) Math.ceil((double)onHand / multiplier);								
								}
							}									

							ParScanStockBean orderData = new ParScanStockBean();

							orderData.setItemGuid(rs.getString("ITEM_GUID"));
							orderData.setParAreaGuid(data.getParArea());
							orderData.setCustomer(data.getShipTo());
							orderData.setItemID(rsItem.getString("ITEM_ID"));							
							orderData.setItemDescription(rsItem.getString("DESCRIPTION"));
							orderData.setVendor(vendor);
							orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
							orderData.setCasePackaging(rsItem.getString("CASE_PACKAGING"));
							orderData.setVendorUOM(rsItem.getString("VENDOR_UOM"));
							orderData.setIssueUOM(rsItem.getString("BILL_UOM"));
							if(rsItem.getString("CURRENT_COST") != null){
								orderData.setCost(Double.toString(Double.parseDouble(rsItem.getString("CURRENT_COST"))*reorderUOP));
								orderData.setTotalCost("0");
							}								
							else{
								orderData.setCost("0");
								orderData.setTotalCost("0");	
							}								
							orderData.setCriticalLevel(Integer.toString(criticalLevel));
							orderData.setParLevel(Integer.toString(parLevel));
							orderData.setOnHandQuantity(Integer.toString(onHand));
							orderData.setOnOrderQuantity(Integer.toString(onOrder));
							orderData.setReorderQtyUOI(Integer.toString(reorderUOI));
							orderData.setReorderQtyUOP(Integer.toString(reorderUOP));
							orderData.setAlternateBarcode("");
							if(rsItem.getString("ALTERNATE_BARCODE") != null)
								orderData.setAlternateBarcode(rsItem.getString("ALTERNATE_BARCODE"));
														
							orderList.add(orderData);																
						}					
						stmtItem.close();
					}else{
						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmtItem.setString(1, customer);
						stmtItem.setString(2, rs.getString("ITEM_GUID"));
						ResultSet rsItem = stmtItem.executeQuery();
						
						rsItem.next();
					
						int orderQty = Integer.parseInt(rs.getString("PAR_LEVEL")) - Integer.parseInt(rs.getString("ON_HAND_QUANTITY")) + onOrder;
						if(orderQty > 0){
							String vendor = "";
							int reorderUOP = 0;
							int reorderUOI = 0;
							
							int criticalLevel = Integer.parseInt(rs.getString("CRITICAL_LEVEL"));
							int parLevel = Integer.parseInt(rs.getString("PAR_LEVEL"));
							int onHand = Integer.parseInt(rs.getString("ON_HAND_QUANTITY"));
							
							stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
							stmtVendor.setString(1, customer);
							stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
							ResultSet rsVendor = stmtVendor.executeQuery();
							
							if(rsVendor.next())
								vendor = rsVendor.getString("VENDOR_NAME");
							stmtVendor.close();			
							
							int multiplier = Integer.parseInt(rsItem.getString("MULTIPLIER"));
							if(rsItem.getString("VENDOR_UOM").equalsIgnoreCase(rs.getString("UOM"))){
								//In Vendor UOM
								reorderUOP = parLevel - onHand;
								if(restock){
									criticalLevel = criticalLevel * multiplier;
									parLevel = parLevel * multiplier;
									onHand = onHand * multiplier;	
									reorderUOI = parLevel - onHand;									
								}else{
									reorderUOI = (parLevel*multiplier) - (onHand*multiplier);
								}

							}else{
								//In Issue UOM
								reorderUOI = parLevel - onHand;								
								reorderUOP = (int) Math.ceil((double)parLevel/multiplier - onHand/multiplier);
								if(!restock){
									criticalLevel = (int) Math.ceil((double)criticalLevel / multiplier);
									parLevel = (int) Math.ceil((double)parLevel / multiplier);
									onHand = (int) Math.ceil((double)onHand / multiplier);								
								}								
							}									
							
							ParScanStockBean orderData = new ParScanStockBean();

							orderData.setItemGuid(rs.getString("ITEM_GUID"));
							orderData.setParAreaGuid(data.getParArea());
							orderData.setCustomer(data.getShipTo());
							orderData.setItemID(rsItem.getString("ITEM_ID"));							
							orderData.setItemDescription(rsItem.getString("DESCRIPTION"));
							orderData.setVendor(vendor);
							orderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
							orderData.setCasePackaging(rsItem.getString("CASE_PACKAGING"));
							orderData.setVendorUOM(rsItem.getString("VENDOR_UOM"));
							orderData.setIssueUOM(rsItem.getString("BILL_UOM"));
							if(rsItem.getString("CURRENT_COST") != null){
								orderData.setCost(Double.toString(Double.parseDouble(rsItem.getString("CURRENT_COST"))*reorderUOP));
								orderData.setTotalCost("0");
							}								
							else{
								orderData.setCost("0");
								orderData.setTotalCost("0");	
							}															
							orderData.setCriticalLevel(Integer.toString(criticalLevel));
							orderData.setParLevel(Integer.toString(parLevel));
							orderData.setOnHandQuantity(Integer.toString(onHand));
							orderData.setOnOrderQuantity(Integer.toString(onOrder));
							orderData.setReorderQtyUOI(Integer.toString(reorderUOI));
							orderData.setReorderQtyUOP(Integer.toString(reorderUOP));
							orderData.setAlternateBarcode("");
							if(rsItem.getString("ALTERNATE_BARCODE") != null)
								orderData.setAlternateBarcode(rsItem.getString("ALTERNATE_BARCODE"));
							
							orderList.add(orderData);									
						}					
						stmtItem.close();
					}							
				}				
				stmt.close();
			}				
			
			return orderList;
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getReorderData: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	

	/**
	 * Business Method.
	 * Removes a new par areas for a customer
	 */
	public List getInventorySummary(String customer, ParScanItemBean item, List parList)throws ParScanResidentsEJBException{
		Connection conn = null;
		List stockList = new ArrayList();
		try
		{
			conn = openConnection();
			PreparedStatement stmt;			
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND ITEM_ID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, item.getItemID());
							
			ResultSet rs = stmt.executeQuery();
			double totalQty = 0;
			boolean found = false;
			while (rs.next()) {
				found = false;
							
				for(Iterator parItor = parList.iterator();parItor.hasNext();){
					ParScanStockBean pArea = (ParScanStockBean) parItor.next();
					
					if(pArea.getParAreaGuid().equalsIgnoreCase(rs.getString("PAR_AREA"))){
						found = true;
						break;
					}
				}
				
				if(found){
					ParScanStockBean data = new ParScanStockBean();
				
					data.setItemID(item.getItemID());
					data.setItemDescription(item.getDescription());
					data.setParAreaGuid(rs.getString("PAR_AREA"));
					data.setCost(item.getCurrentCost());
					data.setIssueUOM(item.getBillUom());
					if(rs.getString("UOM").equalsIgnoreCase(item.getBillUom())){
						data.setCriticalLevel(rs.getString("CRITICAL_LEVEL"));
						data.setOnHandQuantity(rs.getString("ON_HAND_QUANTITY"));
						data.setParLevel(rs.getString("PAR_LEVEL"));
						data.setTotalCost(Double.toString(Double.parseDouble(data.getOnHandQuantity())*Double.parseDouble(data.getCost())));
					}else{
						data.setCriticalLevel(Integer.toString(Integer.parseInt(rs.getString("CRITICAL_LEVEL"))*Integer.parseInt(item.getMultiplier())));
						data.setOnHandQuantity(Integer.toString(Integer.parseInt(rs.getString("ON_HAND_QUANTITY"))*Integer.parseInt(item.getMultiplier())));
						data.setParLevel(Integer.toString(Integer.parseInt(rs.getString("PAR_LEVEL"))*Integer.parseInt(item.getMultiplier())));
						data.setTotalCost(Double.toString(Double.parseDouble(data.getOnHandQuantity())*Double.parseDouble(data.getCost())));					
					}
				
					stockList.add(data);	
				}
			}												
			stmt.close();							
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getInventorySummary: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
		return stockList;	
	}
		
	/**
	 * Business Method.
	 * Removes a new par areas for a customer
	 */
	public List getResidentUsageAnalysis(String customer, List items, Date from, Date to) throws ParScanResidentsEJBException{
		Connection conn = null;
		List analysisList = new ArrayList();
		try
		{
			conn = openConnection();
			PreparedStatement stmt;
			PreparedStatement stmtRes;
			DateTime scheduleEndDate = null;
			DateTime scheduleStartDate = null;			
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			//Get all standard charges
			Timestamp sqlStartDate = new Timestamp(from.getTime());
			Timestamp sqlEndDate = new Timestamp(to.getTime());
			
			Map resMap = getResident(customer);
			
			for(Iterator itor = items.iterator();itor.hasNext();){				
				ParScanItemBean data = (ParScanItemBean) itor.next();
			
				stmtRes = conn.prepareStatement("SELECT DISTINCT RESIDENT_GUID FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND CHARGE_DATE BETWEEN ? AND ?");
				stmtRes.setString(1, customer);
				stmtRes.setTimestamp(2,sqlStartDate);
				stmtRes.setTimestamp(3,sqlEndDate);				
				ResultSet rsRes = stmtRes.executeQuery();
			
				while(rsRes.next()){									
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CHARGE_ID = ? AND CUSTOMER = ? AND CHARGE_DATE BETWEEN ? AND ? AND RESIDENT_GUID = ?");
					stmt.setString(1, data.getItemGUID());
					stmt.setString(2, customer);
					stmt.setTimestamp(3,sqlStartDate);
					stmt.setTimestamp(4,sqlEndDate);
					stmt.setString(5, rsRes.getString("RESIDENT_GUID"));
								
					ResultSet rs = stmt.executeQuery();
					double totalQty = 0;
					while (rs.next()) {
						if(rs.getString("RECURRING_FLAG") == null){
							totalQty = totalQty + Double.parseDouble(rs.getString("QUANTITY"));	
						}
					}												
					stmt.close();				
				
					//Get all recurring charges
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CHARGE_ID = ? AND CUSTOMER = ? AND START_DATE <= ? AND RECURRING_FLAG = 'X' AND RESIDENT_GUID = ?");
			
					stmt.setString(1, data.getItemGUID());
					stmt.setString(2, customer);
					stmt.setTimestamp(3,sqlEndDate);
					stmt.setString(4, rsRes.getString("RESIDENT_GUID"));

					rs = stmt.executeQuery();
					while (rs.next()) {
						DateTime searchStartDate = new DateTime(dateFormat.parse(dateFormat.format(from)).getTime());
						DateTime searchEndDate = new DateTime(dateFormat.parse(dateFormat.format(to)).getTime());
						scheduleStartDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("START_DATE"))).getTime());
				
						if(rs.getTimestamp("END_DATE") != null)
							scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("END_DATE"))).getTime());
						else
							scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())));
				
						if(scheduleEndDate.isAfter(searchStartDate) || scheduleEndDate.equals(searchStartDate)){
							totalQty = totalQty + getRecurringQuantity(rs.getString("FREQUENCY"),Double.parseDouble(rs.getString("QUANTITY")), scheduleStartDate, searchStartDate, scheduleEndDate, searchEndDate);
						}
					}
							
					if(totalQty > 0){
						ParScanResidentBean res = (ParScanResidentBean) resMap.get(rsRes.getString("RESIDENT_GUID"));
						
						if(res != null){
							ParScanItemBean usage = new ParScanItemBean();
							usage.setItemGUID(data.getItemGUID());
							usage.setItemID(data.getItemID());
							usage.setDescription(data.getDescription());
							usage.setBillUom(data.getBillUom());
							usage.setCurrentPrice(data.getCurrentPrice());
							usage.setCurrentCost(data.getCurrentCost());						
							usage.setResidentName(res.getFirstName()+" "+res.getLastName());
							usage.setTotalCharged(outNumberFormat.format(totalQty*Double.parseDouble(usage.getCurrentPrice())));
							usage.setTotalCost(outNumberFormat.format(totalQty*Double.parseDouble(usage.getCurrentCost())));
			
							String costDiff = outNumberFormat.format(Double.parseDouble(usage.getTotalCharged())-Double.parseDouble(usage.getTotalCost()));
							String costPercent = outNumberFormat.format((Double.parseDouble(costDiff)/Double.parseDouble(usage.getTotalCharged()))*100);
							usage.setCostAnalysis(costDiff+" ("+costPercent+"%)");	
							usage.setTotalUsage(Integer.toString((int)Math.ceil(totalQty)));
				
							analysisList.add(usage);							
						}					
					}						
					stmt.close();				
				}
				stmtRes.close();
			}
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getResidentUsageAnalysis: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
		return analysisList;
	}		
		
	/**
	 * Business Method.
	 * Removes a new par areas for a customer
	 */
	public List getParAreaCostAnalysis(String customer, List items, String parArea, Date from, Date to) throws ParScanResidentsEJBException{
		Connection conn = null;
		List analysisList = new ArrayList();
		try
		{
			conn = openConnection();
			PreparedStatement stmt;
			PreparedStatement stmtExtra;
			PreparedStatement stmtData;
			DateTime scheduleEndDate = null;
			DateTime scheduleStartDate = null;			
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			
			for(Iterator itor = items.iterator();itor.hasNext();){				
				ParScanItemBean data = (ParScanItemBean) itor.next();
			
				//Get Item Data
				stmtData = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtData.setString(1, customer);;
				stmtData.setString(2, data.getItemGUID());							
				ResultSet rsItem = stmtData.executeQuery();	
				
				while(rsItem.next()){
					data.setCurrentCost(rsItem.getString("CURRENT_COST"));
					data.setCurrentPrice(rsItem.getString("CURRENT_PRICE"));
					data.setItem(rsItem.getString("VENDOR_ITEM"));		
					data.setBillUom(rsItem.getString("BILL_UOM"));
					data.setMultiplier(rsItem.getString("MULTIPLIER"));
			
					if(rsItem.getString("VENDOR_ID") != null){
						stmtExtra = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
						stmtExtra.setString(1, customer);
						stmtExtra.setString(2, rsItem.getString("VENDOR_ID"));
						ResultSet rsVen = stmtExtra.executeQuery();
						
						if(rsVen.next())
							data.setVendor(rsVen.getString("VENDOR_NAME"));
						else
							data.setVendor("");
						stmtExtra.close();
					}else
						data.setVendor("");

					if(rsItem.getString("CATEGORY_ID") != null){
						stmtExtra = conn.prepareStatement("SELECT * FROM PARSCAN_CATEGORY WHERE CUSTOMER = ? AND ID = ?");
						stmtExtra.setString(1, customer);
						stmtExtra.setString(2, rsItem.getString("CATEGORY_ID"));
						ResultSet rsCat = stmtExtra.executeQuery();
						
						if(rsCat.next())
							data.setCategory(rsCat.getString("PRODUCT_CATEGORY"));
						else
							data.setCategory("");
						stmtExtra.close();
					}else
						data.setCategory("");
			
					//Get all charges
					Timestamp sqlStartDate = new Timestamp(from.getTime());
					DateTime eDate = new DateTime(to.getTime());
					eDate = eDate.plusDays(1);						
					Timestamp sqlEndDate = new Timestamp(new java.util.Date(eDate.getMillis()).getTime());
	
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CUSTOMER = ? AND PAR_AREA = ? AND CHARGE_ID = ? AND CHARGE_DATE BETWEEN ? AND ? ORDER BY CHARGE_DATE ASC");
					stmt.setString(1, customer);
					stmt.setString(2, parArea);
					stmt.setString(3, data.getItemGUID());
					stmt.setTimestamp(4,sqlStartDate);
					stmt.setTimestamp(5,sqlEndDate);

					ResultSet rs = stmt.executeQuery();
					double totalQty = 0;
					while (rs.next()) {
						if(rs.getString("RECURRING_FLAG") == null){
							totalQty = totalQty + Double.parseDouble(rs.getString("QUANTITY"));	
						}else{
							DateTime searchStartDate = new DateTime(dateFormat.parse(dateFormat.format(from)).getTime());
							DateTime searchEndDate = new DateTime(dateFormat.parse(dateFormat.format(to)).getTime());
							scheduleStartDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("START_DATE"))).getTime());
				
							if(rs.getTimestamp("END_DATE") != null)
								scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("END_DATE"))).getTime());
							else
								scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())));
				
							if(scheduleEndDate.isAfter(searchStartDate) || scheduleEndDate.equals(searchStartDate)){
								totalQty = totalQty + getRecurringQuantity(rs.getString("FREQUENCY"),Double.parseDouble(rs.getString("QUANTITY")), scheduleStartDate, searchStartDate, scheduleEndDate, searchEndDate);
							}														
						}
					}												
					stmt.close();		
	
//					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK_LOG WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ? AND UPDATE_TIMESTAMP BETWEEN ? AND ? ORDER BY UPDATE_TIMESTAMP ASC");
//					stmt.setString(1, customer);
//					stmt.setString(2, parArea);
//					stmt.setString(3, data.getItemGUID());
//					stmt.setTimestamp(4,sqlStartDate);
//					stmt.setTimestamp(5,sqlEndDate);
//
//					ResultSet rs = stmt.executeQuery();
//					double totalQty = 0;
//					while (rs.next()) {
//						if(rs.getString("UPDATE_ACTION").equalsIgnoreCase("Charged to Resident")){
//							totalQty = totalQty + Double.parseDouble(rs.getString("UPDATE_QUANTITY"));	
//						}else if(rs.getString("UPDATE_ACTION").equalsIgnoreCase("Returned from Resident")){
//							totalQty = totalQty - Double.parseDouble(rs.getString("UPDATE_QUANTITY"));
//						}
//					}												
//					stmt.close();				
					
					data.setCurrentCost(outNumberFormat.format(Double.parseDouble(data.getCurrentCost())/Double.parseDouble(data.getMultiplier())));		
					data.setTotalCharged(outNumberFormat.format(totalQty*Double.parseDouble(data.getCurrentPrice())));
					data.setTotalCost(outNumberFormat.format(totalQty*Double.parseDouble(data.getCurrentCost())));
				
					String costDiff = outNumberFormat.format(Double.parseDouble(data.getTotalCharged())-Double.parseDouble(data.getTotalCost()));
					String costPercent = outNumberFormat.format((Double.parseDouble(costDiff)/Double.parseDouble(data.getTotalCharged()))*100);
					data.setCostAnalysis(costDiff+" ("+costPercent+"%)");	
					data.setTotalUsage(Integer.toString((int)Math.ceil(totalQty)));
					
					analysisList.add(data);
				}	
				stmtData.close();
			}
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getUsageAnalysis: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
		return analysisList;
	}		
		
	/**
	 * Business Method.
	 * Removes a new par areas for a customer
	 */
	public List getUsageAnalysis(String customer, List items, Date from, Date to) throws ParScanResidentsEJBException{
		Connection conn = null;
		List analysisList = new ArrayList();
		try
		{
			conn = openConnection();
			PreparedStatement stmt;
			DateTime scheduleEndDate = null;
			DateTime scheduleStartDate = null;			
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			
			for(Iterator itor = items.iterator();itor.hasNext();){				
				ParScanItemBean data = (ParScanItemBean) itor.next();
			
				//Get all standard charges
				Timestamp sqlStartDate = new Timestamp(from.getTime());
				Timestamp sqlEndDate = new Timestamp(to.getTime());

				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CHARGE_ID = ? AND CUSTOMER = ? AND CHARGE_DATE BETWEEN ? AND ?");
				stmt.setString(1, data.getItemGUID());
				stmt.setString(2, customer);
				stmt.setTimestamp(3,sqlStartDate);
				stmt.setTimestamp(4,sqlEndDate);
								
				ResultSet rs = stmt.executeQuery();
				double totalQty = 0;
				while (rs.next()) {
					if(rs.getString("RECURRING_FLAG") == null){
						totalQty = totalQty + Double.parseDouble(rs.getString("QUANTITY"));	
					}
				}												
				stmt.close();				
				
				//Get all recurring charges
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_RESIDENT WHERE CHARGE_ID = ? AND CUSTOMER = ? AND START_DATE <= ? AND RECURRING_FLAG = 'X'");
			
				stmt.setString(1, data.getItemGUID());
				stmt.setString(2, customer);
				stmt.setTimestamp(3,sqlEndDate);

				rs = stmt.executeQuery();
				while (rs.next()) {
					DateTime searchStartDate = new DateTime(dateFormat.parse(dateFormat.format(from)).getTime());
					DateTime searchEndDate = new DateTime(dateFormat.parse(dateFormat.format(to)).getTime());
					scheduleStartDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("START_DATE"))).getTime());
				
					if(rs.getTimestamp("END_DATE") != null)
						scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(rs.getTimestamp("END_DATE"))).getTime());
					else
						scheduleEndDate = new DateTime(dateFormat.parse(dateFormat.format(new java.util.Date())));
				
					if(scheduleEndDate.isAfter(searchStartDate) || scheduleEndDate.equals(searchStartDate)){
						totalQty = totalQty + getRecurringQuantity(rs.getString("FREQUENCY"),Double.parseDouble(rs.getString("QUANTITY")), scheduleStartDate, searchStartDate, scheduleEndDate, searchEndDate);
					}
				}
				stmt.close();							
				data.setTotalCharged(outNumberFormat.format(totalQty*Double.parseDouble(data.getCurrentPrice())));
				data.setTotalCost(outNumberFormat.format(totalQty*Double.parseDouble(data.getCurrentCost())));
			
				String costDiff = outNumberFormat.format(Double.parseDouble(data.getTotalCharged())-Double.parseDouble(data.getTotalCost()));
				String costPercent = outNumberFormat.format((Double.parseDouble(costDiff)/Double.parseDouble(data.getTotalCharged()))*100);
				data.setCostAnalysis(costDiff+" ("+costPercent+"%)");	
				data.setTotalUsage(Integer.toString((int)Math.ceil(totalQty)));
				
				analysisList.add(data);	
			}
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getUsageAnalysis: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
		return analysisList;
	}	
	
	/**
	 * Business Method.
	 * Removes a new par areas for a customer
	 */
	public void removeParArea(String customer, List parArea) throws ParScanResidentsEJBException{
		Connection conn = null;

		try
		{
			conn = openConnection();
			PreparedStatement stmtPar;
			
			for(Iterator itor = parArea.iterator();itor.hasNext();){				
				ParScanStockBean data = (ParScanStockBean) itor.next();
				
				stmtPar = conn.prepareStatement("DELETE FROM PARSCAN_PARAREA WHERE CUSTOMER = ? AND PAR_AREA = ?");
				stmtPar.setString(1, customer);
				stmtPar.setString(2, data.getParAreaGuid());
				stmtPar.executeUpdate();				
				stmtPar.close();
				
				stmtPar = conn.prepareStatement("DELETE FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ?");
				stmtPar.setString(1, customer);
				stmtPar.setString(2, data.getParAreaGuid());
				stmtPar.executeUpdate();				
				stmtPar.close();
			}
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during removeParArea: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}		
	
	/**
	 * Business Method.
	 * Adds new products to a product master for a customer
	 */
	public void addProductsFromHistory(String customer, List products) throws ParScanResidentsEJBException{
		Connection conn = null;
					
		try
		{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			PreparedStatement stmtProduct;
			PreparedStatement stmt;
			PreparedStatement stmtVendor;						
						
			for(Iterator itor = products.iterator();itor.hasNext();){
				ParScanItemBean productData = (ParScanItemBean) itor.next();
				
				stmtProduct = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND MEDLINE_ITEM = 'X' AND VENDOR_ITEM = ?");
				stmtProduct.setString(1, customer);
				stmtProduct.setString(2, productData.getItemID());
				
				ResultSet rsProduct = stmtProduct.executeQuery();
				rsProduct.next();
				
				if(rsProduct.getInt("CNT") <= 0){	
					stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
					stmtVendor.setString(1, customer);
					stmtVendor.setString(2, "Medline Industries");
					ResultSet rsVendor = stmtVendor.executeQuery();
					
					rsVendor.next();	
					stmt = conn.prepareStatement("INSERT INTO PARSCAN_ITEM (ID, CUSTOMER, ITEM_ID, VENDOR_ITEM, DESCRIPTION, VENDOR_ID, CATEGORY_ID, VENDOR_UOM, MULTIPLIER, CASE_PACKAGING, CURRENT_COST, FUTURE_COST, FUTURE_COST_DATE, CURRENT_PRICE, FUTURE_PRICE, FUTURE_PRICE_DATE, ALTERNATE_BARCODE, MEDLINE_ITEM, BILL_UOM, PARSCAN_USER, PRICE_CHANGE_DATE, COST_CHANGE_DATE, VENDOR_PREFERENCE, KIT_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");								
					UID uid = new UID(); 
					Timestamp todayDate = new Timestamp(new java.util.Date().getTime());

					stmt.setString(1, uid.toString());
					stmt.setString(2, customer);
					stmt.setString(3, productData.getItemID());
					stmt.setString(4, productData.getItem());
					stmt.setString(5, productData.getDescription());
					stmt.setString(6, rsVendor.getString("ID"));
					stmt.setString(7, null);
					stmt.setString(8, productData.getVendorUom());
					stmt.setString(9, Integer.toString((int) getConversion(productData.getVendorUom(),productData.getItem())));
					
					String casePackaging = Integer.toString((int) getConversion(productData.getVendorUom(),productData.getItem())) + " EA/" + productData.getVendorUom();
					
					stmt.setString(10, casePackaging);
					stmt.setString(11, getMedlinePrice(customer, productData.getItem()));
					stmt.setString(12, null);
					stmt.setTimestamp(13, null);
					stmt.setString(14, null);
					stmt.setString(15, null);				
					stmt.setTimestamp(16, null);
					stmt.setString(17, null);
					stmt.setString(18, "X");
					stmt.setString(19, "EA");
					stmt.setString(20, user);
					stmt.setTimestamp(21, null);
					stmt.setTimestamp(22, todayDate);
					stmt.setInt(23, 1);
					stmt.setString(24, null);
					
					stmt.executeUpdate();
					stmt.close();
					stmtVendor.close();
				}
				stmtProduct.close();
			}										
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during addProductsFromHistory: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	/**
	 * Business Method.
	 * Adds new products to a product master for a customer
	 */	
	public void updateRecurringSchedule(String customer, String residentGUID, ParScanResidentChargeBean data)throws ParScanResidentsEJBException{
		PreparedStatement stmt;	
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			ParScanResidentBean rData = new ParScanResidentBean();
			rData = getGUIDResident(customer, residentGUID);
						
			conn = openConnection();
			Timestamp sqlEndDate = null;
			Timestamp sqlDismissDate = null;
			
			if(!data.getEndDate().equalsIgnoreCase("")){
				sqlEndDate = new Timestamp(dateFormat.parse(data.getEndDate()).getTime());
				if(!rData.getDismissDate().equalsIgnoreCase("")){
					sqlDismissDate = new Timestamp(dateFormat.parse(rData.getDismissDate()).getTime());
				}else{
					sqlDismissDate = sqlEndDate;
				}	
				
				if(sqlEndDate.after(sqlDismissDate))
					sqlEndDate = sqlDismissDate;								
			}else{
				if(!rData.getDismissDate().equalsIgnoreCase(""))
					sqlEndDate = new Timestamp(dateFormat.parse(rData.getDismissDate()).getTime());					 					
			}
					
			stmt = conn.prepareStatement("UPDATE PARSCAN_RESIDENT SET END_DATE = ? WHERE CUSTOMER = ? AND RESIDENT_GUID = ? AND ID = ? AND FREQUENCY = ? AND RECURRING_FLAG = 'X'");
			stmt.setTimestamp(1, sqlEndDate);
			stmt.setString(2, customer);
			stmt.setString(3, residentGUID);
			stmt.setString(4, data.getChargeID());
			stmt.setString(5, data.getFrequency());
			stmt.executeUpdate();
			stmt.close();
		
			conn.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during updateRecurringSchedule: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}
		
	/**
	 * Business Method.
	 * Returns parscan device data
	 */
	public List getDevices() throws ParScanResidentsEJBException{
		List deviceList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtDevice;
		Connection conn = null;
		
		try
			{
				String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();		
				conn = openConnection();

				stmt = conn.prepareStatement("SELECT DISTINCT DEVICE_KEY FROM PARSCAN_DEVICE WHERE PARSCAN_USER = ?");
				stmt.setString(1, user);
				ResultSet rs = stmt.executeQuery();	
				
				while(rs.next()){
					stmtDevice = conn.prepareStatement("SELECT * FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ?");
					stmtDevice.setString(1, rs.getString("DEVICE_KEY"));
					ResultSet rsDevice = stmtDevice.executeQuery();
					rsDevice.next();
					
					ParScanDeviceBean deviceData = new ParScanDeviceBean();
					deviceData.setDeviceKey(rs.getString("DEVICE_KEY"));
					deviceData.setDeviceName(rsDevice.getString("DEVICE_NAME"));
					deviceData.setChargeTracker(rsDevice.getString("CHARGE_TRACKER"));
					deviceData.setSupplyTracker(rsDevice.getString("SUPPLY_TRACKER"));
					deviceData.setPurchaseTracker(rsDevice.getString("PURCHASE_TRACKER"));					
					
					deviceList.add(deviceData);
					stmtDevice.close();				
				}

				stmt.close();			
			}
			catch (Exception ex)
			{
				StringWriter stringWriter = new StringWriter();
				ex.printStackTrace(new PrintWriter(stringWriter));
				ParScanResidentsSSBean.logger.errorT("Error during getDevices: " + stringWriter.toString());
				throw new ParScanResidentsEJBException(ex);
			}		
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	

		return deviceList;
	}	
	
	/**
	 * Business Method.
	 */
	public void createDevice(String deviceName, List accounts, String chargeTracker, String supplyTracker, String purchaseTracker, String dssi) throws ParScanResidentsEJBException{
		Connection conn = null;
		String key = "";
		String newKey = "";
				
		try
		{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();
			PreparedStatement stmt;
			UID uid = new UID(); 
			Timestamp now = new Timestamp(System.currentTimeMillis()); 
			
			key = uid.toString();
			
			for(Iterator itor = accounts.iterator();itor.hasNext();){
				ParScanDeviceBean data = (ParScanDeviceBean) itor.next();

				stmt = conn.prepareStatement("INSERT INTO PARSCAN_DEVICE (ID, DEVICE_KEY, DEVICE_NAME, CUSTOMER, SYNC_REPORT, SYNC_TIME, PARSCAN_USER, CHARGE_TRACKER, SUPPLY_TRACKER, PURCHASE_TRACKER, DSSI_FLAG) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				UID id = new UID(); 
				stmt.setString(1, id.toString());
				stmt.setString(2, key);
				stmt.setString(3, deviceName);
				stmt.setString(4, data.getSoldTo());
				stmt.setString(5, "New ParScan Device");
				stmt.setTimestamp(6, now);
				stmt.setString(7,user);
				stmt.setString(8,emptyString(chargeTracker));
				stmt.setString(9,emptyString(supplyTracker));
				stmt.setString(10,emptyString(purchaseTracker));
				stmt.setString(11,emptyString(dssi));
				
				stmt.executeUpdate();
				
				stmt.close();
			}
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during Create Device: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	/**
	 * Business Method.
	 */
	public void editDevice(ParScanDeviceBean device, String dssi) throws ParScanResidentsEJBException{
		Connection conn = null;
			
		try
		{
			conn = openConnection();
			PreparedStatement stmt;
			
			stmt = conn.prepareStatement("UPDATE PARSCAN_DEVICE SET DEVICE_NAME = ?, CHARGE_TRACKER = ?, SUPPLY_TRACKER = ?, PURCHASE_TRACKER = ?, DSSI_FLAG = ? WHERE DEVICE_KEY = ?");
			stmt.setString(1, device.getDeviceName());
			stmt.setString(2, emptyString(device.getChargeTracker()));
			stmt.setString(3, emptyString(device.getSupplyTracker()));
			stmt.setString(4, emptyString(device.getPurchaseTracker()));
			stmt.setString(5, emptyString(dssi));
			stmt.setString(6, device.getDeviceKey());
			stmt.executeUpdate();
			
			stmt.close();
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during Edit Device: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	

	/**
	 * Business Method.
	 * Returns parscan device data
	 */
	public String getDeviceDSSI(String key) throws ParScanResidentsEJBException{
		PreparedStatement stmt;
		Connection conn = null;
		String dssi = "0";
		
		try
			{		
				conn = openConnection();

				stmt = conn.prepareStatement("SELECT DSSI_FLAG FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ?");
				stmt.setString(1, key);
				ResultSet rs = stmt.executeQuery();	
				
				while(rs.next()){
					dssi = rs.getString("DSSI_FLAG");					
				}

				stmt.close();			
			}
			catch (Exception ex)
			{
				StringWriter stringWriter = new StringWriter();
				ex.printStackTrace(new PrintWriter(stringWriter));
				ParScanResidentsSSBean.logger.errorT("Error during getDeviceDSSI: " + stringWriter.toString());
				throw new ParScanResidentsEJBException(ex);
			}		
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	

		return dssi;
	}

	/**
	 * Business Method.
	 * Returns customer's ship to list
	 */
	public Customer getAccountDetails(String customer) throws ParScanResidentsEJBException{				
		Customer parScanData = new Customer();
		
		try{
			InitialContext	ic = new InitialContext();
				
			CustomerSearchSSLocalHome custLocalHome = (CustomerSearchSSLocalHome) ic.lookup("localejbs/medline.com/com.medline.srp.CustomersEAR/CustomerSearchSSBean");
			CustomerSearchSSLocal custLocal = custLocalHome.create();			
					
			List shipToListData = null;		
			shipToListData = custLocal.customerDetails(customer);			
							
			for(Iterator itor = shipToListData.iterator();itor.hasNext();){
				CustomerDetailsBean data = (CustomerDetailsBean) itor.next();
					
				parScanData.setCustomerNumber(data.getCustomerNumber());
				parScanData.setCustomerName(data.getCustomerName1());
				parScanData.setStreet(data.getStreet());
				parScanData.setCity(data.getCity());
				parScanData.setState(data.getState());
				parScanData.setZipCode(data.getZipcode());
				parScanData.setPhone(data.getPhone());
				break;
			}			
		}			catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getCustomerShip-to: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		
		return parScanData;
	}	
	
	/**
	 * Business Method.
	 */
	public List getUserCustomers() throws ParScanResidentsEJBException{
		return getUserCustomers("");
	}	
		
	/**
	 * Business Method.
	 */
	public List getUserCustomers(String device) throws ParScanResidentsEJBException{
		ArrayList customerResult = new ArrayList();
		
		try {
			InitialContext ctx = new InitialContext();

			ZgetCustomersbyUserAliasService obj = (ZgetCustomersbyUserAliasService) ctx.lookup("java:comp/env/CustomerProxy");
			ZgetCustomersbyUserAlias port = (ZgetCustomersbyUserAlias) obj.getLogicalPort();
                  
			UserData ud = new UserData();
			ud.setAlias(myContext.getCallerPrincipal().getName());
			ud.setRepId("");
			ud.setSearchTerm("");
            
			com.medline.esa.customer.proxy.types.List[] customerList = port.getCustomersbyUserAlias(ud).getList();                  

			for (int i = 0; i < customerList.length; i++) {
				Customer c = new Customer();
				c.setCustomerName(customerList[i].getCustomerName().getName1());
				c.setCustomerNumber(customerList[i].getCustomerNumber());
				customerResult.add(c);
			}	
																	
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during Get User Customers: " + stringWriter.toString());
			throw new ParScanResidentsEJBException();
		}

		if (!device.equalsIgnoreCase("")){
			PreparedStatement stmt;
			Connection conn = null;
			int i = 0;
			
			try
				{
					conn = openConnection();
								
					stmt = conn.prepareStatement("SELECT DISTINCT CUSTOMER FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ? ORDER BY CUSTOMER ASC");
					stmt.setString(1,device);			
				
					ResultSet rs = stmt.executeQuery();
					while(rs.next()){														
						i = 0;
						for(Iterator itor = customerResult.iterator();itor.hasNext();){
							Customer customerData = (Customer) itor.next();			
							
							if((padLineNumber(customerData.getCustomerNumber())).equalsIgnoreCase(rs.getString("CUSTOMER"))){
								customerResult.remove(i);
								break;
							}	
							i++;							
						}
					}
					stmt.close();				
				}
				catch (Exception ex)
				{
					StringWriter stringWriter = new StringWriter();
					ex.printStackTrace(new PrintWriter(stringWriter));
					ParScanResidentsSSBean.logger.errorT("Error during getUserCustomers: " + stringWriter.toString());
					throw new ParScanResidentsEJBException(ex);
				}	
			finally{
				try {
					if (conn != null)conn.close();
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
			}
		}	
		return customerResult;		
	}		
	
	public List getUserCustomerList(String user) throws ParScanResidentsEJBException{
		ArrayList customerResult = new ArrayList();
		
		try {
			InitialContext ctx = new InitialContext();

			ZgetCustomersbyUserAliasService obj = (ZgetCustomersbyUserAliasService) ctx.lookup("java:comp/env/CustomerProxy");
			ZgetCustomersbyUserAlias port = (ZgetCustomersbyUserAlias) obj.getLogicalPort();
                  
			UserData ud = new UserData();
			ud.setAlias(user);
			ud.setRepId("");
			ud.setSearchTerm("");
            
			com.medline.esa.customer.proxy.types.List[] customerList = port.getCustomersbyUserAlias(ud).getList();                  

			for (int i = 0; i < customerList.length; i++) {
				Customer c = new Customer();
				c.setCustomerName(customerList[i].getCustomerName().getName1());
				c.setCustomerNumber(customerList[i].getCustomerNumber());
				customerResult.add(c);
			}
														
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during Get User Customers: " + stringWriter.toString());
			throw new ParScanResidentsEJBException();
		}
		
		return customerResult;
	}	
	
	/**
	 * Business Method.
	 */
	public SyncData getDeviceList(String userId) throws ParScanResidentsEJBException{
		SyncData syncData = new SyncData();
		Connection conn = null;
		List deviceList = new ArrayList();
		
		try {
			conn = openConnection();
			PreparedStatement stmt;
			PreparedStatement stmtDevice;
			
			InitialContext ctx = new InitialContext();


			//check group access
			com.sap.security.api.IUser user = UMFactory.getUserFactory().getUserByLogonID(userId);//UMFactory.getAuthenticator().getLoggedInUser();
			IGroup gAccess = UMFactory.getGroupFactory().getGroupByUniqueName("ParScanResidents");
			boolean parScanResidents = (gAccess.isUserMember(user.getUniqueID(), true));

			stmt = conn.prepareStatement("SELECT DISTINCT DEVICE_KEY FROM PARSCAN_DEVICE WHERE PARSCAN_USER = ?");
			stmt.setString(1, userId);
			ResultSet rs = stmt.executeQuery();	
				
			while(rs.next()){
				stmtDevice = conn.prepareStatement("SELECT * FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ?");
				stmtDevice.setString(1, rs.getString("DEVICE_KEY"));
				ResultSet rsDevice = stmtDevice.executeQuery();
				
				while(rsDevice.next()){
					ParScanDeviceBean deviceData = new ParScanDeviceBean();
					
					if(parScanResidents)
						deviceData.setIsResident("1");
					else
						deviceData.setIsResident("0");
					deviceData.setDeviceKey(rs.getString("DEVICE_KEY"));
					deviceData.setDeviceName(rsDevice.getString("DEVICE_NAME"));
					deviceData.setChargeTracker(rsDevice.getString("CHARGE_TRACKER"));
					deviceData.setSupplyTracker(rsDevice.getString("SUPPLY_TRACKER"));
					deviceData.setPurchaseTracker(rsDevice.getString("PURCHASE_TRACKER"));
					deviceData.setSoldTo(rsDevice.getString("CUSTOMER"));						
					Customer customer = getAccountDetails(deviceData.getSoldTo());
					deviceData.setSoldToName(customer.getCustomerName());
					deviceData.setIsConsignment(checkConsignment(deviceData.getSoldTo()));
					
					deviceList.add(deviceData);				
				}					
				stmtDevice.close();				
			}

			stmt.close();	

			ParScanDeviceBean[] deviceResults = new ParScanDeviceBean[deviceList.size()];
			deviceResults = (ParScanDeviceBean[]) deviceList.toArray(deviceResults);
			syncData.setDeviceData(deviceResults);
			
			
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during Get Device List: " + stringWriter.toString());
			throw new ParScanResidentsEJBException();
		}finally{
		try {
			if (conn != null)conn.close();
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
	}	
		
		return syncData;
	}	
	
	/**
	 * Business Method.
	 */
	public SyncData syncParScanDevice(String deviceKey, String user, String employeeData, String poData, String residentData, String stockData) throws ParScanResidentsEJBException{
		Connection conn = null;
		SyncData syncData = new SyncData();
		List parAreaList = new ArrayList();
		List stockList = new ArrayList();
		List employeeList = new ArrayList();
		List orderList = new ArrayList();
		List residentList = new ArrayList();
		List serviceList = new ArrayList();
		List deviceList = new ArrayList();
		List gtins = new ArrayList();			
		PreparedStatement stmtKey;
		PreparedStatement stmt;
		PreparedStatement stmtDevice;
		boolean activeResidents = true;
		
		try
		{			
			conn = openConnection();
						
			stmtKey = conn.prepareStatement("SELECT DISTINCT CUSTOMER FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ?");
			stmtKey.setString(1, deviceKey);
			ResultSet rsCustomer = stmtKey.executeQuery();
			
			stmtDevice = conn.prepareStatement("SELECT * FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ?");
			stmtDevice.setString(1, deviceKey);
			ResultSet rsDevice = stmtDevice.executeQuery();
			
			while(rsDevice.next()){
				ParScanDeviceBean deviceData = new ParScanDeviceBean();
				deviceData.setChargeTracker(rsDevice.getString("CHARGE_TRACKER"));
				deviceData.setSupplyTracker(rsDevice.getString("SUPPLY_TRACKER"));
				deviceData.setPurchaseTracker(rsDevice.getString("PURCHASE_TRACKER"));
				deviceData.setSoldTo(rsDevice.getString("CUSTOMER"));
				Customer customer = getAccountDetails(deviceData.getSoldTo());
				deviceData.setSoldToName(customer.getCustomerName());
				deviceData.setIsConsignment(checkConsignment(deviceData.getSoldTo()));
														
				deviceList.add(deviceData);
			}
			stmtDevice.close();
			
			while(rsCustomer.next()){
				String customer = rsCustomer.getString("CUSTOMER");
				
				if(employeeData.equalsIgnoreCase("Y")){
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_EMPLOYEE WHERE CUSTOMER = ?");
					stmt.setString(1, customer);
					ResultSet rs = stmt.executeQuery();	
					
					while(rs.next()){
						ParScanEmployeeBean employee = new ParScanEmployeeBean();
						
						employee.setId(rs.getString("EMPLOYEE_ID"));
						employee.setFirstName(rs.getString("FIRST_NAME"));
						employee.setLastName(rs.getString("LAST_NAME"));
						employee.setCustomer(customer);
						employeeList.add(employee);
					}
					stmt.close();								
				}				
							
				if(residentData.equalsIgnoreCase("Y")){
					
					List resList = getAllResidents(customer, activeResidents);
					for(Iterator itor = resList.iterator();itor.hasNext();){
						ParScanResidentBean resident = (ParScanResidentBean) itor.next();
						resident.setCustomer(customer);
						residentList.add(resident);
					}
					
					List serList = getServices(customer);
					for(Iterator itor = serList.iterator();itor.hasNext();){
						ParScanServiceBean service = (ParScanServiceBean) itor.next();
						service.setCustomer(customer);
						serviceList.add(service);
					}	
				}
				
				if(poData.equalsIgnoreCase("Y")){
					java.util.Date date = new java.util.Date();
					Timestamp sqlToDate = new Timestamp(date.getTime());						
					conn = openConnection();
					DateTime eDate = new DateTime(date.getTime());
					eDate = eDate.minusDays(30);			
					Timestamp sqlFromDate = new Timestamp(new java.util.Date(eDate.getMillis()).getTime());
					
					stmt = conn.prepareStatement("SELECT DISTINCT PO_NUMBER FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND RECEIVE_FLAG = ? AND CREATE_DATE BETWEEN ? AND ?");
					stmt.setString(1, customer);
					stmt.setString(2,"0");
					stmt.setTimestamp(3, sqlFromDate);
					stmt.setTimestamp(4, sqlToDate);					
					ResultSet rs = stmt.executeQuery();	
					
					while(rs.next()){
						List poDetailsList = getScanPODetailsList(customer, rs.getString("PO_NUMBER"));
						for(Iterator itor = poDetailsList.iterator();itor.hasNext();){
							ParScanPOBean order = (ParScanPOBean) itor.next();
							order.setCustomer(customer);
							order.setPoNumber(rs.getString("PO_NUMBER"));
							orderList.add(order);
						}
					}
					stmt.close();				
				}				
				
				if(residentData.equalsIgnoreCase("Y") || stockData.equalsIgnoreCase("Y")){
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PARAREA WHERE CUSTOMER = ?");
					stmt.setString(1, customer);
					ResultSet rs = stmt.executeQuery();
					
					while(rs.next()){
						String parArea = rs.getString("PAR_AREA");
						
						ParScanParAreaBean parData = new ParScanParAreaBean();
						parData.setSoldTo(customer);
						parData.setParArea(parArea);						
						parAreaList.add(parData);
						
						List stockDataList = getDeviceStock(customer, parArea);
						for(Iterator itor = stockDataList.iterator();itor.hasNext();){
							ParScanStockBean stock = (ParScanStockBean) itor.next();
							stock.setCustomer(customer);
							stockList.add(stock);
						}	
					}
					stmt.close();
				}
				
			}

			stmtKey.close();
			
			gtins.addAll( getGtins( deviceKey ) );
			
			Timestamp now = new Timestamp(System.currentTimeMillis());
			stmt = conn.prepareStatement("UPDATE PARSCAN_DEVICE SET SYNC_REPORT = ?, SYNC_TIME = ? WHERE DEVICE_KEY = ?");
			
			stmt.setString(1, "Synchronized ParScan Device");
			stmt.setTimestamp(2, now);
			stmt.setString(3, deviceKey);
			stmt.executeUpdate();
			stmt.close();
			
			ParScanDeviceBean[] deviceResults = new ParScanDeviceBean[deviceList.size()];
			deviceResults = (ParScanDeviceBean[]) deviceList.toArray(deviceResults);
			syncData.setDeviceData(deviceResults);
			
			ParScanParAreaBean[] parAreaResults = new ParScanParAreaBean[parAreaList.size()];
			parAreaResults = (ParScanParAreaBean[]) parAreaList.toArray(parAreaResults);
			syncData.setParAreaData(parAreaResults);

			ParScanResidentBean[] residentResults = new ParScanResidentBean[residentList.size()];
			residentResults = (ParScanResidentBean[]) residentList.toArray(residentResults);
			syncData.setResidentData(residentResults);	
			
			ParScanPOBean[] poResults = new ParScanPOBean[orderList.size()];
			poResults = (ParScanPOBean[]) orderList.toArray(poResults);
			syncData.setPoData(poResults);			
			
			ParScanEmployeeBean[] employeeResults = new ParScanEmployeeBean[employeeList.size()];
			employeeResults = (ParScanEmployeeBean[]) employeeList.toArray(employeeResults);
			syncData.setEmployeeData(employeeResults);			
			
			ParScanServiceBean[] serviceResults = new ParScanServiceBean[serviceList.size()];
			serviceResults = (ParScanServiceBean[]) serviceList.toArray(serviceResults);
			syncData.setServiceData(serviceResults);			
			
			ParScanStockBean[] stockResults = new ParScanStockBean[stockList.size()];
			stockResults = (ParScanStockBean[]) stockList.toArray(stockResults);
			syncData.setStockData(stockResults);
			
			GtinData[] gtinData = new GtinData[ gtins.size() ];
			gtinData = ( GtinData[] ) gtins.toArray( gtinData );
			syncData.setGtinData( gtinData );
			
			logger.debugT("syncData.setGtinData(gtinData), gtinData.length = " + gtinData.length);
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during Sync Device: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	

		return syncData;
	}	
	
	private List getScanPODetailsList(String customer, String po) throws ParScanResidentsEJBException {
		List poList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtVendor;
		PreparedStatement stmtItem;
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

		try {
			conn = openConnection();
			int remainQty = 0;
			
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? ORDER BY ITEM_ID");
			stmt.setString(1, customer);
			stmt.setString(2, po);
			
			ResultSet rs = stmt.executeQuery();			
			while (rs.next()) {				
				remainQty = 0;				
				remainQty = Integer.parseInt(rs.getString("QUANTITY")) - Integer.parseInt(rs.getString("RECEIVE_QUANTITY"));				
				
				if(remainQty > 0){
					ParScanPOBean data = new ParScanPOBean();				
					stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
					stmtVendor.setString(1, customer);
					stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
					ResultSet rsVendor = stmtVendor.executeQuery();
					rsVendor.next();

					data.setVendor(rsVendor.getString("VENDOR_NAME"));
					data.setOrderCost(rs.getString("COST"));
					data.setParArea(rs.getString("PAR_AREA"));
					data.setOrderQuantity(Integer.toString(remainQty));
					data.setReceiveQuantity("0");
					data.setOrderTotal(Double.toString(Double.parseDouble(data.getOrderCost())*Double.parseDouble(data.getOrderQuantity())));
					data.setCreateDate(dateFormat.format(rs.getTimestamp("CREATE_DATE")));
					data.setReceiveFlag(rs.getString("RECEIVE_FLAG"));
					data.setUOM(rs.getString("UOM"));
					if(rs.getTimestamp("RECEIVE_DATE") != null)
						data.setReceiveDate(dateFormat.format(rs.getTimestamp("RECEIVE_DATE")));
					else
						data.setReceiveDate("");
					data.setProductGUID(rs.getString("ITEM_GUID"));

					stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmtItem.setString(1, customer);
					stmtItem.setString(2, data.getProductGUID());
			
					ResultSet rsItem = stmtItem.executeQuery();
				
					while(rsItem.next()){
						data.setProductID(rsItem.getString("ITEM_ID"));
						data.setDescription(rsItem.getString("DESCRIPTION"));						
						data.setCasePakcaging(rsItem.getString("CASE_PACKAGING"));
						data.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
					}
				
					data.setSummary(buildPOItemSummary(customer, po, data));
				
					stmtVendor.close();
					stmtItem.close();
					poList.add(data);				
				}
			}
			stmt.close();

			return poList;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getScanPODetailsList: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}
	
	private List getDeviceStock(String customer, String parArea)throws ParScanResidentsEJBException{
		List productList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtItem;				
		Connection conn = null;
		String status = "";
		
		try{
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ?");
			stmt.setString(1,customer);
			stmt.setString(2,parArea);
			
			ResultSet rs = stmt.executeQuery();
			
			while (rs.next()) {
				ParScanStockBean data = new ParScanStockBean();
						
				stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmtItem.setString(1,customer);
				stmtItem.setString(2,rs.getString("ITEM_GUID"));
				
				ResultSet rsProduct = stmtItem.executeQuery();
				rsProduct.next();
				
				String parLevel = rs.getString("PAR_LEVEL");
				String criticalLevel = rs.getString("CRITICAL_LEVEL");
				String onHand = rs.getString("ON_HAND_QUANTITY");
				//put into Issue UOM
				if(!rs.getString("UOM").equalsIgnoreCase(rsProduct.getString("BILL_UOM"))){
					parLevel = Integer.toString((int)Double.parseDouble(parLevel)*Integer.parseInt(rsProduct.getString("MULTIPLIER")));
					criticalLevel = Integer.toString((int)Double.parseDouble(criticalLevel)*Integer.parseInt(rsProduct.getString("MULTIPLIER")));
					onHand = Integer.toString((int)Double.parseDouble(onHand)*Integer.parseInt(rsProduct.getString("MULTIPLIER")));
				}
				
				data.setItemGuid(rs.getString("ITEM_GUID"));
				data.setItemID(rsProduct.getString("ITEM_ID"));
				data.setItemDescription(rsProduct.getString("DESCRIPTION"));
				data.setIssueUOM(rsProduct.getString("BILL_UOM"));
				data.setVendorUOM(rsProduct.getString("VENDOR_UOM"));
				data.setMultiplier(rsProduct.getString("MULTIPLIER"));
				data.setCasePackaging(rsProduct.getString("CASE_PACKAGING"));
				data.setCriticalLevel(criticalLevel);
				data.setParLevel(parLevel);
				data.setOnHandQuantity(onHand);
				data.setParAreaGuid(parArea);
				data.setAlternateBarcode(rsProduct.getString("ALTERNATE_BARCODE"));
				
				productList.add(data);
				
				stmtItem.close();
			}
			stmt.close();					
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getDeviceStock: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			

		return productList;	
	}
	
	private void updateAlternateBarCode( ParScanStockBean[] stockData)throws ParScanResidentsEJBException
	{
		String methodName = "updateAlternateBarCode()";
		logger.entering( methodName );
		
		Connection con = null;
		PreparedStatement pstmt = null;

		try
		{
			con = openConnection();
			
			con.setAutoCommit( false );
	
			String query = sqlProp.getString("updateAlternateBarCode");
	
			pstmt = con.prepareStatement( query );
			
			for( int i = 0; i < stockData.length; i++)
			{
				ParScanStockBean stockBean = stockData[i];
				
				if( StringUtils.isNotEmpty( stockBean.getAlternateBarcode() ) )
				{
					pstmt.setString( 1, stockBean.getAlternateBarcode() );
					pstmt.setString( 2, stockBean.getCustomer() );
					pstmt.setString( 3, stockBean.getItemGuid() );
					
					logger.debugT("Alternate barcode =" + stockBean.getAlternateBarcode() + ", customer = " + stockBean.getCustomer()
												+" and item guid = " + stockBean.getItemGuid());
					
					pstmt.addBatch();
				}
			}			
			
			pstmt.executeBatch();
			
			con.commit();
			
			
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during updateAlternateBarCode: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally
		{
			closeDbResources(con, pstmt);
		}
	}
	
	private List getGtins(String deviceKey) throws ParScanResidentsEJBException
	{
		String methodName = "getGtins()";
		logger.entering( methodName );
		
		List gtins = new ArrayList();
		
		Connection con = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			
			con = openConnection();
			
			String query = sqlProp.getString("getGtins");
			
			pstmt = con.prepareStatement( query );
			pstmt.setString( 1, deviceKey );
			
			rs = pstmt.executeQuery();
			
			while( rs.next() )
			{
				GtinData gtin = new GtinData();
				
				gtin.setEanCategory( rs.getString("EAN_CATEGORY") );
				gtin.setGtin( rs.getString("GTIN") );
				gtin.setItemId( rs.getString("ITEM_ID") );
				gtin.setUom( rs.getString("UOM") );
				
				gtins.add( gtin );
			}

		}	
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getGtins: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally
		{
			closeDbResources(con, pstmt, rs);
		}
		
		logger.debugT("gtins.size() = " + gtins.size());
		logger.exiting( methodName );
		
		return gtins;			

	}
	
	public UpdateGtinResponse updateGtin(UpdateGtinRequest request)
	{
		String methodName = "updateGtin()";
		logger.entering( methodName );
		
		int returnCode = 0;
		String errorMsg = "";
		
		GtinData gintData[] = request.getGtinData();
		
		ArrayList insertGtins = new ArrayList();
		ArrayList updateGtins = new ArrayList();
		ArrayList deleteGtins = new ArrayList();
		
		logger.debugT("gintData.length = " + gintData.length);
		
		Connection con = null;
		
		try
		{
			con = openConnection();
			
			con.setAutoCommit( false );
	
			for(int i = 0; i < gintData.length; i++)
			{
				GtinData gtin = gintData[i];
				
				logger.debugT("Current gtin = " + gtin);
				
				if(gtin.getDeleteGtin())
				{
					deleteGtins.add(gtin);
				}
				else if(doesGtinExist(con, gtin))
				{
					updateGtins.add(gtin);
				}
				else
				{
					insertGtins.add(gtin);
				}
				
			}
			
			if( !insertGtins.isEmpty() )
			{
				insertGtins(con, insertGtins);
			}
			
			if( !updateGtins.isEmpty() )
			{
				updateGtins(con, updateGtins);
			}
						
			if( !deleteGtins.isEmpty() )
			{
				deleteGtins(con, deleteGtins);
			}
			
			con.commit();
		}
		catch (Exception ex)
		{
			try
			{
				con.rollback();
			}
			catch(SQLException e)
			{
				logger.errorT("Failed to rollback transaction: " + e.toString());
			}
			
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			
			ParScanResidentsSSBean.logger.errorT("Error during updateGtin: " + stringWriter.toString());
				
			returnCode = -1;
			errorMsg = "Error during updateGtin: " + ex.toString();
		}
		finally
		{
			try
			{
				if(con != null)
				{
					con.close();
				}
			}
			catch(SQLException e)
			{
				logger.errorT("Error while closing connection : " + e.toString());			
			}
		}
			
		UpdateGtinResponse response = new UpdateGtinResponse();
		response.setErrorMessage( errorMsg );
		response.setReturnCode( returnCode );
		
		return response;
		
	}
	
	/**
	 * Business Method.
	 */
	public String updateServer(String deviceKey, SyncData updateData, String user, boolean createOrder, boolean consignOrder, boolean lastCall) throws ParScanResidentsEJBException{
		String response = "Update failed. Please contact your system administrator";
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			boolean dssiOrders = false;
			boolean reqs = false;
			String dssiCustomer = "";
			java.util.Date stockDate = new java.util.Date();
			Timestamp todayStamp = new Timestamp(stockDate.getTime());			
			java.util.Date date = new java.util.Date();
			date = dateFormat.parse(dateFormat.format(date));
			Timestamp now = new Timestamp(date.getTime());
			conn = openConnection();
			PreparedStatement stmt;
			PreparedStatement stmtInfo;
			PreparedStatement stmtUpdate;
			PreparedStatement stmtDevice;
			ResultSet rs;
			List parList = new ArrayList();
			
			
			ParScanResidentChargeBean[] chargeData = updateData.getChargeData();
			ParScanStockBean[] stockData = updateData.getStockData();
			ParScanStockBean[] transferData = updateData.getTransferData();
			ParScanStockBean[] orderData = updateData.getOrderData();
			ParScanEmployeeScanBean[] logData = updateData.getScanLogData();
			ParScanPOBean[] poData = updateData.getPoData();			
			
			if(logData != null){
				for(int i = 0; i < logData.length; i++){
					if(isInteger(logData[i].getChargeQuantity())){
						UID uid = new UID();
						stmt = conn.prepareStatement("INSERT INTO PARSCAN_SCAN_LOG (ID, CUSTOMER, EMPLOYEE_ID, RESIDENT_GUID, SCAN_DATE, DEVICE_KEY, CHARGE_ID, CHARGE_QUANTITY, SERVICE_FLAG, RESIDENT_ID, FIRST_NAME, LAST_NAME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?)");
						stmt.setString(1, uid.toString());
						stmt.setString(2, logData[i].getCustomer());
						stmt.setString(3, logData[i].getEmployeeID());
						stmt.setString(4, logData[i].getResidentGUID());
						stmt.setTimestamp(5, now);
						stmt.setString(6, deviceKey);
						stmt.setString(7, logData[i].getChargeID());
						stmt.setString(8, logData[i].getChargeQuantity());
						stmt.setString(9, emptyString(logData[i].getServiceFlag()));
						stmt.setString(10, logData[i].getResidentID());
						stmt.setString(11, logData[i].getResidentFirstName());
						stmt.setString(12, logData[i].getResidentLastName());
						stmt.executeUpdate();
						stmt.close();						
					}
				}
			}
			
			if(poData != null){
				String poNumber = "";		
				String customer = "";
				List poItemList = new ArrayList();
				for(int i = 0; i < poData.length; i++){
					ParScanFillUpBean data = new ParScanFillUpBean();						
					data.setOrderQty(poData[i].getOrderQuantity());
					data.setReceiveQty(poData[i].getReceiveQuantity());
					data.setItemGuid(poData[i].getProductGUID());
					data.setItemID(poData[i].getProductID());
					data.setParArea(poData[i].getParArea());
					poItemList.add(data);
					
					receiveSelectedItems(poData[i].getCustomer(), poData[i].getPoNumber(), poItemList);
					poItemList.clear();					
				}	
	
			}
			
			if(chargeData != null){
				for(int i = 0; i < chargeData.length; i++){
					if(!chargeData[i].getLastName().equalsIgnoreCase("NEW")){
						if(isInteger(chargeData[i].getQuantity())){
							
							addCharge(chargeData[i].getCustomer(), chargeData[i].getResidentGUID(), chargeData[i]);
						}
					}
				}	
			}	

			if(stockData != null){
				for(int i = 0; i < stockData.length; i++){					
					if(stockData[i].getScanned().equalsIgnoreCase("X")){
						if(isInteger(stockData[i].getOnHandQuantity())){
							stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
							stmt.setString(1, stockData[i].getCustomer());
							stmt.setString(2, stockData[i].getItemGuid());
							stmt.setString(3, stockData[i].getParAreaGuid());
							rs = stmt.executeQuery();
							if(rs.next()){
								String stockUOM = rs.getString("UOM");
								String criticalLevel = rs.getString("CRITICAL_LEVEL");
								String parLevel = rs.getString("PAR_LEVEL");
								stmt.close();
								double multiplier = Double.parseDouble(stockData[i].getMultiplier());
				
								if(stockData[i].getScannedUOM().equalsIgnoreCase("IOM")){
									if(!stockData[i].getIssueUOM().equalsIgnoreCase(stockUOM)){
										//Stock in Vendor, take to ISSUE
										parLevel = Integer.toString((int) (multiplier * Double.parseDouble(parLevel)));
										criticalLevel = Integer.toString((int) (multiplier * Double.parseDouble(criticalLevel)));
									}				
								}else{
									if(!stockData[i].getVendorUOM().equalsIgnoreCase(stockUOM)){
										//Stock in Issue, take to VENDOR
										parLevel = Integer.toString((int) (Double.parseDouble(parLevel) / multiplier ));
										criticalLevel = Integer.toString((int) (Double.parseDouble(criticalLevel) / multiplier ));							
									}
								}
				
								UID uid = new UID();			
								stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
								stmt.setString(1, uid.toString());
								stmt.setString(2, stockData[i].getCustomer());
								stmt.setString(3,stockData[i].getItemGuid());
								stmt.setString(4,stockData[i].getParAreaGuid());
								stmt.setString(5,stockData[i].getOnHandQuantity());
								if(stockData[i].getScannedUOM().equalsIgnoreCase("IOM"))
									stmt.setString(6, stockData[i].getIssueUOM());
								else
									stmt.setString(6, stockData[i].getVendorUOM());
								stmt.setString(7, "Inventory Count");	
								stmt.setTimestamp(8, todayStamp);	
								stmt.setString(9, "*");	
								stmt.executeUpdate();
								stmt.close();				
				
								//update stock table
								stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CRITICAL_LEVEL = ?, PAR_LEVEL = ?, UOM = ?, CHANGE_ACTION = ?, CHANGE_TIMESTAMP = ?, SCANNED = 'X' WHERE CUSTOMER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
								stmt.setString(1,stockData[i].getOnHandQuantity());
								stmt.setString(2, criticalLevel);
								stmt.setString(3, parLevel);
								if(stockData[i].getScannedUOM().equalsIgnoreCase("IOM"))
									stmt.setString(4, stockData[i].getIssueUOM());
								else
									stmt.setString(4, stockData[i].getVendorUOM());
								stmt.setString(5, "Stock Change: Device Scan Count");					
								stmt.setTimestamp(6, now);
								stmt.setString(7, stockData[i].getCustomer());
								stmt.setString(8, stockData[i].getItemGuid());
								stmt.setString(9, stockData[i].getParAreaGuid());
								stmt.executeUpdate();
								stmt.close();	
																			
								stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
								stmt.setString(1, stockData[i].getCustomer());
								stmt.setString(2, stockData[i].getItemGuid());
								ResultSet rsKit = stmt.executeQuery();
								rsKit.next();
								if(rsKit.getString("KIT_FLAG") != null){
									stmt.close();
									stmt = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND KIT_ID = ?");
									stmt.setString(1, stockData[i].getCustomer());
									stmt.setString(2, stockData[i].getItemID());
									rsKit = stmt.executeQuery();
		
									while(rsKit.next()){
										stmtUpdate = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CRITICAL_LEVEL = ?, PAR_LEVEL = ?, CHANGE_ACTION = ?, CHANGE_TIMESTAMP = ?, SCANNED = 'X' WHERE CUSTOMER = ? AND ITEM_GUID = ? AND PAR_AREA = ?");
										stmtUpdate.setString(1,Integer.toString(Integer.parseInt(stockData[i].getOnHandQuantity())*Integer.parseInt(rs.getString("KIT_QUANTITY"))));
										stmtUpdate.setString(2,Integer.toString(Integer.parseInt(criticalLevel)*Integer.parseInt(rs.getString("KIT_QUANTITY"))));				
										stmtUpdate.setString(3,Integer.toString(Integer.parseInt(parLevel)*Integer.parseInt(rs.getString("KIT_QUANTITY"))));		
										stmt.setString(4, "Stock Change: Device Scan Count");					
										stmt.setTimestamp(5, now);		
										stmtUpdate.setString(6,stockData[i].getCustomer());											
										stmtUpdate.setString(7, rsKit.getString("ITEM_GUID"));
										stmtUpdate.setString(8, stockData[i].getParAreaGuid());						
										stmtUpdate.executeUpdate();
										stmtUpdate.close();						
									}				
								}
								stmt.close();																	
							}							
						}				
					}
				}
				//update alternate bar code
				updateAlternateBarCode(stockData);
				
			}
			
			if(transferData != null){
				for(int i = 0; i < transferData.length; i++){
					if(isInteger(transferData[i].getTransferQuantity()))
						transferServerProducts(transferData[i].getCustomer(), transferData[i].getParAreaGuid(), transferData[i].getTransferArea(), transferData[i]);
				}
			}
								
			if(orderData != null){
				//check group access
				com.sap.security.api.IUser iuser = UMFactory.getUserFactory().getUserByLogonID(user);
				IGroup gAccess = UMFactory.getGroupFactory().getGroupByUniqueName("ParScanRequisitions");
				reqs = (gAccess.isUserMember(iuser.getUniqueID(), true));				   

				if(reqs){
					createScanParScanReqCopy(orderData);
				}else{
					if(orderData.length > 0)						
						createScanOrder(orderData);	
					
					for(int i = 0; i < orderData.length; i++){
						dssiCustomer = orderData[i].getCustomer();
							
						stmtDevice = conn.prepareStatement("SELECT * FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ? AND CUSTOMER = ?");
						stmtDevice.setString(1, deviceKey);
						stmtDevice.setString(2, dssiCustomer);
						ResultSet rsDevice = stmtDevice.executeQuery();
				
						while(rsDevice.next()){
							if(rsDevice.getString("DSSI_FLAG").equalsIgnoreCase("1"))
								dssiOrders = true;
							break;
						}
						stmtDevice.close();
						break;						
					}				
				}							
			}			
			
			if(createOrder && lastCall){				
				java.util.Date poDate = new java.util.Date();				
				String strDate = dateFormat.format(poDate);
				
				if(reqs){
					
				}else if(dssiOrders){
					createDSSIOrders(dssiCustomer);
				}else{
					QueueConnectionFactory queueConnectionFactory = null;
					QueueConnection queueConnection = null;
					QueueSession queueSession = null;
					Queue queue = null;
					QueueSender queueSender = null;
					ObjectMessage message = null;

					try {
					   //factory for Jms connections   		
						queueConnectionFactory=(QueueConnectionFactory)getJNDIContext().lookup("jmsfactory/default/QueueConnectionFactory");
	
					   //get the Jms queue		   	
						queue=(Queue)getJNDIContext().lookup("jmsqueues/default/ParscanImportQueue");
	
					   //get the Jms Connection from the facotry		
						queueConnection = queueConnectionFactory.createQueueConnection();
	
					   //establish a Jms seesion		
						queueSession =	queueConnection.createQueueSession(false, QueueSession.AUTO_ACKNOWLEDGE);
	
					   //create a sender component (for a message)		
						queueSender = queueSession.createSender(queue);
	
					   //create a message component
						message = queueSession.createObjectMessage();
						message.setStringProperty("placeOrder", "TRUE");		
						message.setStringProperty("deviceKey", deviceKey);
						message.setStringProperty("poDate", strDate);
						message.setStringProperty("userID", user);
						if(consignOrder)
							message.setStringProperty("consignOrder", "TRUE");
						else
							message.setStringProperty("consignOrder", "FALSE"); 	 		
	
						//write it		
						queueSender.send(message);		
					} catch (NamingException e) {
						
						e.printStackTrace();
					} catch (Exception e) {
						
						e.printStackTrace();
					}
					finally {

					if(queueSession!=null)
						try {
							queueSession.close();
						} catch (JMSException e1) {
							
							e1.printStackTrace();
						}	

					if(queueConnection!=null)	
					//close the Jms Connection so it can be reused by factory
						try {
							queueConnection.close();
						} catch (JMSException e2) {
							
							e2.printStackTrace();
						}
					}				
				}															
			}
			stmt = conn.prepareStatement("UPDATE PARSCAN_DEVICE SET SYNC_REPORT = ?, SYNC_TIME = ? WHERE DEVICE_KEY = ?");			
			stmt.setString(1, "Updated ParScan Server");
			stmt.setTimestamp(2, now);
			stmt.setString(3, deviceKey);
			stmt.executeUpdate();
			stmt.close();
			
			response = "ParScan server updated successfully.";		
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during Update Server: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
				return response;
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
		
		return response;
	}	
	
	private static InitialContext getJNDIContext() throws Exception
	{
		Hashtable env = new Hashtable();
		env.put("domain", "true");
		InitialContext ctx=new InitialContext(env);
		
		return ctx;
	}	
	
	public void placeScanOrder(String deviceKey, String poDate, boolean consignOrder, String userID)throws ParScanResidentsEJBException{
		List poList = new ArrayList();
		PreparedStatement stmt;
		PreparedStatement stmtPO;
		PreparedStatement stmtItem;
		PreparedStatement stmtDevice;
		Connection conn = null;
		boolean placeOrder = false;

		try {			
			conn = openConnection();

			stmtDevice = conn.prepareStatement("SELECT * FROM PARSCAN_DEVICE WHERE DEVICE_KEY = ?");
			stmtDevice.setString(1, deviceKey);
			ResultSet rsDevice = stmtDevice.executeQuery();
			
			while(rsDevice.next()){
				placeOrder = false;
				String customer = rsDevice.getString("CUSTOMER");
				
				stmtPO = conn.prepareStatement("SELECT DISTINCT SHIP_TO FROM PARSCAN_ORDER WHERE CUSTOMER = ?");
				stmtPO.setString(1, customer);
				
				ResultSet rsShip = stmtPO.executeQuery();
				
				while(rsShip.next()){
					String po = "Medl"+rsShip.getString("SHIP_TO").substring(3)+"-"+poDate.replaceAll("/","");

					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ? AND SENT_MEDLINE = '0' ORDER BY ITEM_ID");
					stmt.setString(1, customer);
					stmt.setString(2, po);

					ResultSet rs = stmt.executeQuery();

					while (rs.next()) {
						placeOrder = true;
						ParScanPOBean data = new ParScanPOBean();

						data.setPoNumber(po);
						data.setShipTo(rs.getString("SHIP_TO"));
						data.setOrderQuantity(rs.getString("QUANTITY"));
						data.setReceiveQuantity("0");
						data.setProductID(rs.getString("ITEM_ID"));
						data.setProductGUID(rs.getString("ITEM_GUID"));
						data.setParArea(rs.getString("PAR_AREA"));

						stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmtItem.setString(1, customer);
						stmtItem.setString(2, data.getProductGUID());
			
						ResultSet rsItem = stmtItem.executeQuery();
				
						while(rsItem.next()){
							data.setUOM(rsItem.getString("VENDOR_UOM"));
							data.setVendorNumber(rsItem.getString("VENDOR_ITEM"));
						}
				
						stmtItem.close();
						poList.add(data);
					}
					stmt.close();		
				
					if(placeOrder){
						stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET SENT_MEDLINE = '1' WHERE CUSTOMER = ? AND PO_NUMBER = ?");
						stmt.setString(1, customer);
						stmt.setString(2, po);
						stmt.executeUpdate();
						stmt.close();						
						
						String orderType = "TA";
				
						if(checkConsignment(customer).equalsIgnoreCase("X") && consignOrder)
							orderType = "KB";
						//if(consignOrder)
								
							
						if(atgUser(customer, userID))		
							createATGOrder(poList, customer, orderType, userID);
						else
							createMedlineOrder(poList, customer, orderType);
						
						poList.clear();						
					}						
				}				
			}
			stmtDevice.close();
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during placeScanOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}
	
	/**
	 * Business Method.
	 */	
	public void createFreeTypeOrder(String customer, String po, String vendorGUID, String medorder, List itemList)throws ParScanResidentsEJBException{
		Connection conn = null;
		PreparedStatement stmt;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			
		try{
			conn = openConnection();
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			java.util.Date utilDate = new java.util.Date();
			Timestamp sqlDate = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());			
			
			stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
			stmt.setString(1, customer);
			stmt.setString(2, po);
			ResultSet rs = stmt.executeQuery();
			rs.next();
			if(rs.getInt("CNT") <= 0){
				stmt.close();
				stmt = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM PARSCAN_FREE_ORDER WHERE CUSTOMER = ? AND PO_NUMBER = ?");
				stmt.setString(1, customer);
				stmt.setString(2, po);
				rs = stmt.executeQuery();
				rs.next();
				if(rs.getInt("CNT") <= 0){
					stmt.close();
					for(Iterator itor = itemList.iterator();itor.hasNext();){
						ParScanPOBean item = (ParScanPOBean) itor.next();
						
						stmt = conn.prepareStatement("INSERT INTO PARSCAN_FREE_ORDER (ID, CUSTOMER, PO_NUMBER, VENDOR_GUID, SHIP_TO, ITEM, DESCRIPTION, QUANTITY, UOM, COST, CREATE_DATE, RECEIVE_FLAG, PARSCAN_USER, RECEIVE_QUANTITY) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
						UID uid = new UID();
						stmt.setString(1, uid.toString());
						stmt.setString(2, customer);
						stmt.setString(3, item.getPoNumber());
						stmt.setString(4, vendorGUID);
						stmt.setString(5, item.getShipTo());
						stmt.setString(6, item.getVendorNumber());
						stmt.setString(7, emptyString(item.getDescription()));
						stmt.setString(8, item.getOrderQuantity());
						stmt.setString(9, emptyString(item.getUOM()));
						stmt.setString(10, emptyString(item.getOrderCost()));
						stmt.setTimestamp(11, sqlDate);
						stmt.setString(12, "0");
						stmt.setString(13, user);
						stmt.setString(14, "0");
						stmt.executeUpdate();
						stmt.close();
					}	
					
					if(medorder.equalsIgnoreCase("X")){
						String orderType = "TA";
				
						if(checkConsignment(customer).equalsIgnoreCase("X"))
							orderType = "KB";
										
						if(atgUser(customer, myContext.getCallerPrincipal().getName()))		
							createATGOrder(consolidateOrder(itemList), customer, orderType, myContext.getCallerPrincipal().getName());
						else
							createMedlineOrder(itemList, customer, orderType);						
					}				
				}
			}			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createFreeTypeOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}
	
	private void createScanOrder(ParScanStockBean[] orderData)throws ParScanResidentsEJBException{
		Connection conn = null;
		PreparedStatement stmt;
		PreparedStatement stmtItem;
		PreparedStatement stmtVendor;
		List kitList = new ArrayList();
		List parAreaList = new ArrayList();
		List poList = new ArrayList();
		List orderList = new ArrayList();
		Map tmpMap = new HashMap();
		Map vendorMap = new HashMap();
		Map shipMap = new HashMap();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			conn = openConnection();
			java.util.Date date = new java.util.Date();
			String strDate = dateFormat.format(date);

			//Add kit items to stock list
			for(int i = 0; i < orderData.length; i++){
				if(isInteger(orderData[i].getOrderQuantity())){
					kitList.add(orderData[i]);
				
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmt.setString(1, orderData[i].getCustomer());
					stmt.setString(2, orderData[i].getItemGuid());
					ResultSet rsKit = stmt.executeQuery();
					if(rsKit.next()){
						if(rsKit.getString("KIT_FLAG") != null){
							String itemID = rsKit.getString("ITEM_ID");
							stmt.close();
							stmt = conn.prepareStatement("SELECT * FROM PARSCAN_KIT WHERE CUSTOMER = ? AND KIT_ID = ?");
							stmt.setString(1, orderData[i].getCustomer());
							stmt.setString(2, itemID);
							rsKit = stmt.executeQuery();
		
							while(rsKit.next()){
								ParScanStockBean data = new ParScanStockBean();
						
								data.setParAreaGuid(orderData[i].getParAreaGuid());
								data.setCustomer(orderData[i].getCustomer());
								data.setItemGuid(rsKit.getString("ITEM_GUID"));
								data.setOrderQuantity(Integer.toString(Integer.parseInt(orderData[i].getOrderQuantity())*Integer.parseInt(rsKit.getString("KIT_QUANTITY"))));
								kitList.add(data);
							}	
							stmt.close();				
						}else
						stmt.close();
					}else
						stmt.close();				
				}
			}			
			
			orderData = new ParScanStockBean[kitList.size()];
			orderData = (ParScanStockBean[]) kitList.toArray(orderData);
			
			//Get Par Area/Ship-to list
			for(int i = 0; i < orderData.length; i++){
				if(isInteger(orderData[i].getOrderQuantity())){
					ParScanStockBean data = (ParScanStockBean) tmpMap.get(orderData[i].getParAreaGuid());
				
					if(data == null){
						tmpMap.put(orderData[i].getParAreaGuid(), orderData[i]);
					
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PARAREA WHERE CUSTOMER = ? AND PAR_AREA = ?");
						stmt.setString(1,orderData[i].getCustomer());
						stmt.setString(2,orderData[i].getParAreaGuid());
						ResultSet rs = stmt.executeQuery();
	
						while(rs.next()){
							ParScanPOBean parData = new ParScanPOBean();
							parData.setParArea(orderData[i].getParAreaGuid());
							parData.setShipTo(rs.getString("SHIP_TO"));
							parData.setCustomer(orderData[i].getCustomer());
							parAreaList.add(parData);
						}	
					}				
				}						
			}	
			
			//Get PO List and create Order
			for(Iterator itor = parAreaList.iterator();itor.hasNext();){
				ParScanPOBean parData = (ParScanPOBean) itor.next();
				
				for(int i = 0; i < orderData.length; i++){
					if(isInteger(orderData[i].getOrderQuantity())){
						if(orderData[i].getParAreaGuid().equalsIgnoreCase(parData.getParArea())){			
							String shipToData = (String) shipMap.get(parData.getShipTo());
					
							if(shipToData == null){
								shipMap.put(parData.getShipTo(), parData.getShipTo());
								vendorMap.clear();
							}				
						
							stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE PAR_AREA = ? AND CUSTOMER = ? AND ITEM_GUID = ?");														
							stmt.setString(1, parData.getParArea());
							stmt.setString(2,parData.getCustomer());
							stmt.setString(3,orderData[i].getItemGuid());						
							ResultSet rs = stmt.executeQuery();
					
							while(rs.next()){
								stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
								stmtItem.setString(1, parData.getCustomer());
								stmtItem.setString(2, orderData[i].getItemGuid());
								ResultSet rsItem = stmtItem.executeQuery();
								rsItem.next();
							
								if(rsItem.getString("KIT_FLAG") == null){
									if(rs.getString("VENDOR_GUID") != null){
										stmtVendor = conn.prepareStatement("SELECT VENDOR_NAME FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND ID = ?");
										stmtVendor.setString(1, orderData[i].getCustomer());
										stmtVendor.setString(2, rs.getString("VENDOR_GUID"));
										ResultSet rsVendor = stmtVendor.executeQuery();
										rsVendor.next();
										String vendor = rsVendor.getString("VENDOR_NAME");							
										String vendorData = (String) vendorMap.get(vendor);
						
										if(vendorData == null){
											ParScanPOBean poData = new ParScanPOBean();
											vendorMap.put(vendor, vendor);
							
											poData.setCustomer(parData.getCustomer());
											poData.setShipTo(parData.getShipTo());
											poData.setVendor(vendor);
											if(vendor.equalsIgnoreCase("Medline Industries"))
												poData.setPoNumber("Medl"+parData.getShipTo().substring(3)+"-"+strDate.substring(0,10).replaceAll("/",""));
											else{
												if(vendor.length() > 4)										
													poData.setPoNumber(vendor.substring(0,3)+parData.getShipTo().substring(3)+"-"+strDate.substring(0,10).replaceAll("/",""));
												else{
													poData.setPoNumber(vendor+parData.getShipTo().substring(3)+"-"+strDate.substring(0,10).replaceAll("/",""));
												}												
											}
																													
											poList.add(poData);
										}	

										stmtVendor.close();	
									}										
								}						
								stmtItem.close();							
							}
							stmt.close();	
						}					
					}
				}				
			}
			
			//Create Orders
			for(Iterator poItor = poList.iterator();poItor.hasNext();){
				ParScanPOBean poData = (ParScanPOBean) poItor.next();

				for(Iterator itor = parAreaList.iterator();itor.hasNext();){
					ParScanPOBean data = (ParScanPOBean) itor.next();

					for(int i = 0; i < orderData.length; i++){
						if(isInteger(orderData[i].getOrderQuantity())){
							if(poData.getShipTo().equalsIgnoreCase(data.getShipTo()) && orderData[i].getParAreaGuid().equalsIgnoreCase(data.getParArea())){				
								stmtVendor = conn.prepareStatement("SELECT ID FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
								stmtVendor.setString(1, data.getCustomer());
								stmtVendor.setString(2, poData.getVendor());
								ResultSet rsVendor = stmtVendor.executeQuery();
								rsVendor.next();
							
								stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND VENDOR_GUID = ? AND ITEM_GUID = ?");					
								stmt.setString(1, data.getCustomer());
								stmt.setString(2, data.getParArea());
								stmt.setString(3, rsVendor.getString("ID"));
								stmt.setString(4, orderData[i].getItemGuid());
								ResultSet rs = stmt.executeQuery();
							
								while(rs.next()){
									stmtItem = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
									stmtItem.setString(1, data.getCustomer());
									stmtItem.setString(2, orderData[i].getItemGuid());
									ResultSet rsItem = stmtItem.executeQuery();
									rsItem.next();
						
									if(rsItem.getString("KIT_FLAG") == null){									
										//Convert to vendor UOM
										int orderQty = 0;
										if(rsItem.getString("VENDOR_UOM").equalsIgnoreCase(orderData[i].getScannedUOM())){
											orderQty = Integer.parseInt(orderData[i].getOrderQuantity());										
										}else{
											double multiplier = Double.parseDouble(rsItem.getString("MULTIPLIER"));
											orderQty = (int) Math.ceil(Double.parseDouble(orderData[i].getOrderQuantity())/multiplier);
										}									

										ParScanPOBean newOrderData = new ParScanPOBean();	
										newOrderData.setPoNumber(poData.getPoNumber());
										newOrderData.setParArea(data.getParArea());
										newOrderData.setProductGUID(rs.getString("ITEM_GUID"));
										newOrderData.setProductID(rsItem.getString("ITEM_ID"));
										newOrderData.setVendor(poData.getVendor());
										newOrderData.setOrderQuantity(Integer.toString((int)orderQty));
										newOrderData.setOrderCost(rsItem.getString("CURRENT_COST"));
										newOrderData.setShipTo(poData.getShipTo());
										newOrderData.setUOM(rsItem.getString("VENDOR_UOM"));
										newOrderData.setVendorNumber(rsItem.getString("VENDOR_ITEM"));									
										orderList.add(newOrderData);									
																		
										stmtItem.close();																		
									}																		
								}
													
								stmtVendor.close();
								stmt.close();
							}						
						}
					}
				}

				createScanPO(orderList, poData.getCustomer());
				orderList.clear();
			}			
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createScanOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}

	private void createScanPO(List orderList, String customer)throws ParScanResidentsEJBException{
		PreparedStatement stmtPO;
		PreparedStatement stmtVendor;
		PreparedStatement stmtStock;	
		String po = "";		
		Connection conn = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		
		try{
			conn = openConnection();	
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			java.util.Date utilDate = new java.util.Date();
			Timestamp sqlDate = new Timestamp(dateFormat.parse(dateFormat.format(utilDate)).getTime());

			for(Iterator itor = orderList.iterator();itor.hasNext();){
				ParScanPOBean data = (ParScanPOBean) itor.next();
				po = data.getPoNumber();
				
				stmtVendor = conn.prepareStatement("SELECT * FROM PARSCAN_VENDOR WHERE CUSTOMER = ? AND VENDOR_NAME = ?");
				stmtVendor.setString(1, customer);
				stmtVendor.setString(2, data.getVendor());
				ResultSet rsVendor = stmtVendor.executeQuery();
				rsVendor.next();
				
				stmtPO = conn.prepareStatement("INSERT INTO PARSCAN_ORDER (ID, CUSTOMER, PO_NUMBER, ITEM_GUID, VENDOR_GUID, QUANTITY, COST, CREATE_DATE, SHIP_TO, PARSCAN_USER, ITEM_ID, PAR_AREA, RECEIVE_FLAG, SENT_MEDLINE, RECEIVE_QUANTITY, UOM) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				UID uid = new UID();									
				stmtPO.setString(1, uid.toString());
				stmtPO.setString(2, customer);
				stmtPO.setString(3, data.getPoNumber());
				stmtPO.setString(4,data.getProductGUID());
				stmtPO.setString(5, rsVendor.getString("ID"));
				stmtPO.setString(6,data.getOrderQuantity());
				stmtPO.setString(7, data.getOrderCost());
				stmtPO.setTimestamp(8, sqlDate);
				stmtPO.setString(9, data.getShipTo());
				stmtPO.setString(10, user);
				stmtPO.setString(11, data.getProductID());
				stmtPO.setString(12, data.getParArea());
				stmtPO.setString(13, "0");
				stmtPO.setString(14, "0");
				stmtPO.setString(15,"0");	
				stmtPO.setString(16,data.getUOM());
				stmtPO.executeUpdate();
				stmtPO.close();
				stmtVendor.close();

				stmtStock = conn.prepareStatement("UPDATE PARSCAN_STOCK SET SCANNED = 'N' WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_ID = ?");
				stmtStock.setString(1, customer);
				stmtStock.setString(2, data.getParArea());
				stmtStock.setString(3, data.getProductID());
				stmtStock.executeUpdate();
				stmtStock.close();		
			}					
						
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createScanPO: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}			
	}
	
	private void createDSSIOrders(String customer) throws ParScanResidentsEJBException
	{
		StringBuffer sb = new StringBuffer();
		List orderList = null;
		PreparedStatement stmt = null;
			
		String po = "";		
		Connection con = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
				
		try
		{
			con = openConnection();
			
			java.util.Date stockDate = new java.util.Date();
			stockDate = dateFormat.parse(dateFormat.format(stockDate));
			Timestamp todayStamp = new Timestamp(stockDate.getTime());
			
			stmt = con.prepareStatement("SELECT DISTINCT PO_NUMBER FROM PARSCAN_ORDER WHERE CUSTOMER = ? AND CREATE_DATE >= ?");
			stmt.setString(1,customer);
			stmt.setTimestamp(2, todayStamp);				
			
			ResultSet rsPO = stmt.executeQuery();
			
			while( rsPO.next() )
			{
				po = rsPO.getString("PO_NUMBER");
	
				orderList = getPODetailsList(customer, po);
				if(isPRDEnvironment())
				{
					sb.append("<order><testorder>N</testorder>");
				}
				else
				{
					sb.append("<order><testorder>Y</testorder>");
				}		
				sb.append("<locationid>"+customer+"</locationid>");
				sb.append("<idname>"+po+"</idname>");
				sb.append("<specialinstruction1></specialinstruction1><specialinstruction2></specialinstruction2>");
				sb.append("<items>");
			
				for(Iterator itor = orderList.iterator();itor.hasNext();){
					ParScanPOBean data = (ParScanPOBean) itor.next();
					
					sb.append("<item>");
					sb.append("<itemnumber>"+data.getVendorNumber()+"</itemnumber>");
					sb.append("<quantity>"+data.getOrderQuantity()+"</quantity>");
					sb.append("</item>");
				}
									
				sb.append("</items>");
				sb.append("</order>");
							
				ParScanConfigJndi config = new ParScanConfigJndi();
				config.load();

				DssiConnector dssiConnector = new DssiConnectorHttpClient( config );
				String response = dssiConnector.sendRequest("orderXML="+sb.toString());
				
				if(logger.beDebug())
				{
					logger.debugT("Payload : "+sb.toString());
					logger.debugT( "Dssi response = "+response);
				}
						
				sb.delete(0, sb.length());
				orderList.clear();				
			}	
			stmt.close();														
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during createDSSIOrder: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally
		{
			try 
			{
				if (con != null)
				{
					con.close();
				} 
			} 
			catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}	
	}
		
	private String checkConsignment(String customer) throws ParScanResidentsEJBException{
		String isConsignment = "";
		
		try
		{
			InitialContext ic = new InitialContext();

			CustomerSearchSSLocalHome custLocalHome =
				(CustomerSearchSSLocalHome) ic.lookup(
					"localejbs/medline.com/com.medline.srp.CustomersEAR/CustomerSearchSSBean");
			CustomerSearchSSLocal custLocal = custLocalHome.create();

			List detailData = null;
			detailData = custLocal.customerDetails(customer);

			for (Iterator itor = detailData.iterator(); itor.hasNext();)
			{
				CustomerDetailsBean data = (CustomerDetailsBean) itor.next();

				isConsignment = data.getConsignment();

				break;
			}
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during checkConsignment: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}		
		return isConsignment;
	}	

	/**
	 * Business Method.
	 */
	public List getPiggyBackList(String customer, List itemList)throws ParScanResidentsEJBException{
		List piggyList = new ArrayList();
		Connection conn = null;
		PreparedStatement stmt;
		
		try {
			conn = openConnection();
			
			for(Iterator itor = itemList.iterator();itor.hasNext();){ 
				ParScanPOBean data = (ParScanPOBean) itor.next();
				ParScanItemBean item = new ParScanItemBean();
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getProductGUID());
				ResultSet rs = stmt.executeQuery();
								
				if(rs.next()){
					if(rs.getString("BILL_UOM").equalsIgnoreCase(data.getUOM())){
						item.setItemID(data.getProductID());
						item.setDescription(data.getDescription());
						item.setBillUom(data.getUOM());
						item.setPigyBackQuantity(Integer.parseInt(data.getOrderQuantity()));
						item.setAlternateBarcode(rs.getString("ALTERNATE_BARCODE"));
					}else{
						item.setItemID(data.getProductID());
						item.setDescription(data.getDescription());
						item.setBillUom(rs.getString("BILL_UOM"));
						item.setPigyBackQuantity(Integer.parseInt(data.getOrderQuantity())*Integer.parseInt(rs.getString("MULTIPLIER")));
						item.setAlternateBarcode(rs.getString("ALTERNATE_BARCODE"));					
					}
					
					piggyList.add(item);
				}		
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getPiggyBackList: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					
					e.printStackTrace();
				}
		}
		return piggyList;				
	}
	
	/**
	 * Business Method.
	 */	
	public void transferProducts(String customer, String currentArea, String transferArea, List productList)throws ParScanResidentsEJBException{
		Connection conn = null;
		List addList = new ArrayList();
		try{
			java.util.Date today = new java.util.Date();
			Timestamp todayStamp = new Timestamp(today.getTime());
			conn = openConnection();
			PreparedStatement stmt;

			for(Iterator itor = productList.iterator();itor.hasNext();){
				ParScanStockBean data = (ParScanStockBean) itor.next();

				UID uid = new UID();			
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3,data.getItemGuid());
				stmt.setString(4,currentArea);
				stmt.setString(5,data.getTransferQuantity());
				stmt.setString(6,data.getCurrentUOM());
				stmt.setString(7, TRANSFERED_TO + transferArea);	
				stmt.setTimestamp(8, todayStamp);	
				stmt.setString(9, "-");	
				stmt.executeUpdate();
				stmt.close();
				
				uid = new UID();			
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
				stmt.setString(1, uid.toString());
				stmt.setString(2, customer);
				stmt.setString(3,data.getItemGuid());
				stmt.setString(4,transferArea);
				stmt.setString(5,data.getTransferQuantity());
				stmt.setString(6,data.getCurrentUOM());
				stmt.setString(7, TRANSFERED_FROM + currentArea);	
				stmt.setTimestamp(8, todayStamp);	
				stmt.setString(9, "+");	
				stmt.executeUpdate();
				stmt.close();
				
				//Update current Par Area
				stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
				stmt.setString(1, data.getOnHandQuantity());
				stmt.setTimestamp(2, todayStamp);
				stmt.setString(3, TRANSFERED_TO + transferArea);				
				stmt.setString(4, customer);
				stmt.setString(5, currentArea);
				stmt.setString(6, data.getItemGuid());
				stmt.executeUpdate();
				stmt.close();					
				
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, transferArea);
				stmt.setString(3, data.getItemGuid());
				ResultSet rs = stmt.executeQuery();				
				
				if(rs.next()){
					if(data.getCurrentUOM().equalsIgnoreCase(rs.getString("UOM"))){
						double handQty = Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) + Double.parseDouble(data.getTransferQuantity()); 
						stmt.close();
						
						stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
						stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
						stmt.setTimestamp(2, todayStamp);
						stmt.setString(3, TRANSFERED_FROM + currentArea);						
						stmt.setString(4, customer);
						stmt.setString(5, transferArea);
						stmt.setString(6, data.getItemGuid());
						stmt.executeUpdate();
						stmt.close();
					}else{
						double handQty = Double.parseDouble(rs.getString("ON_HAND_QUANTITY"));
						double par = Double.parseDouble(rs.getString("PAR_LEVEL"));
						double reorder = Double.parseDouble(rs.getString("CRITICAL_LEVEL"));
						stmt.close();
						stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
						stmt.setString(1, customer);
						stmt.setString(2, data.getItemGuid());
						rs = stmt.executeQuery();
						rs.next();
						
						double multiplier = Double.parseDouble(rs.getString("MULTIPLIER"));
						if(data.getCurrentUOM().equalsIgnoreCase(rs.getString("BILL_UOM"))){
						//convert transferArea handQty, criticalLevel, parLevel, UOM to UOI; add transferQty
							handQty = handQty*multiplier + Double.parseDouble(data.getTransferQuantity());
							par = par*multiplier;
							reorder = reorder*multiplier;
							
							stmt.close();
							stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, PAR_LEVEL = ?, CRITICAL_LEVEL = ?, UOM = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
							stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
							stmt.setString(2, Integer.toString((int)Math.ceil(par)));
							stmt.setString(3, Integer.toString((int)Math.ceil(reorder)));
							stmt.setString(4, data.getCurrentUOM());
							stmt.setTimestamp(5, todayStamp);
							stmt.setString(6, TRANSFERED_FROM + currentArea);							
							stmt.setString(7, customer);
							stmt.setString(8, transferArea);
							stmt.setString(9, data.getItemGuid());
							stmt.executeUpdate();
							stmt.close();							
						}else{							
						//convert currentArea handQty to UOI for addition
							data.setTransferQuantity(Integer.toString((int)Math.ceil(Double.parseDouble(data.getTransferQuantity())*multiplier)));
							handQty = handQty + Double.parseDouble(data.getTransferQuantity());
							
							stmt.close();
							stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
							stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
							stmt.setTimestamp(2, todayStamp);
							stmt.setString(3, TRANSFERED_FROM + currentArea);
							stmt.setString(4, customer);
							stmt.setString(5, transferArea);
							stmt.setString(6, data.getItemGuid());
							stmt.executeUpdate();
							stmt.close();							
						}
					}										
				}else{
					data.setUpdateAction(TRANSFERED_FROM + currentArea);
					data.setScanned("N");
					data.setOnHandQuantity(data.getTransferQuantity());
					addList.add(data);
					addParAreaProducts(customer, transferArea, addList, true);
					addList.clear();
				}						
			}			
					
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during transferProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	/**
	 * Business Method.
	 */	
	private void transferServerProducts(String customer, String currentArea, String transferArea, ParScanStockBean data)throws ParScanResidentsEJBException{
		Connection conn = null;
		List addList = new ArrayList();
		try{
			double newHandQty = 0;
			java.util.Date today = new java.util.Date();
			java.sql.Timestamp todayStamp = new Timestamp(today.getTime());
			conn = openConnection();
			PreparedStatement stmt;

			UID uid = new UID();			
			stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
			stmt.setString(1, uid.toString());
			stmt.setString(2, customer);
			stmt.setString(3,data.getItemGuid());
			stmt.setString(4,currentArea);
			stmt.setString(5,data.getTransferQuantity());
			stmt.setString(6,data.getCurrentUOM());
			stmt.setString(7, TRANSFERED_TO + transferArea);	
			stmt.setTimestamp(8, todayStamp);	
			stmt.setString(9, "-");	
			stmt.executeUpdate();
			stmt.close();
				
			uid = new UID();			
			stmt = conn.prepareStatement("INSERT INTO PARSCAN_STOCK_LOG (ID,CUSTOMER,ITEM_GUID,PAR_AREA,UPDATE_QUANTITY,UPDATE_UOM,UPDATE_ACTION,UPDATE_TIMESTAMP,UPDATE_SIGN) VALUES (?,?,?,?,?,?,?,?,?)");
			stmt.setString(1, uid.toString());
			stmt.setString(2, customer);
			stmt.setString(3,data.getItemGuid());
			stmt.setString(4,transferArea);
			stmt.setString(5,data.getTransferQuantity());
			stmt.setString(6,data.getCurrentUOM());
			stmt.setString(7, TRANSFERED_FROM + currentArea);	
			stmt.setTimestamp(8, todayStamp);	
			stmt.setString(9, "+");	
			stmt.executeUpdate();
			stmt.close();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, currentArea);
			stmt.setString(3, data.getItemGuid());
			ResultSet rs = stmt.executeQuery();
						
			if(rs.next()){
				newHandQty = Double.parseDouble(rs.getString("ON_HAND_QUANTITY"));
				data.setCriticalLevel(rs.getString("CRITICAL_LEVEL"));
				data.setParLevel(rs.getString("PAR_LEVEL"));
				if(data.getCurrentUOM().equalsIgnoreCase(rs.getString("UOM"))){
					if(Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) >= Double.parseDouble(data.getTransferQuantity())){
						newHandQty = newHandQty - Double.parseDouble(data.getTransferQuantity()); 
					}else{
						data.setTransferQuantity(Integer.toString((int)Math.ceil(newHandQty)));
						newHandQty = 0;
					}
				}else{
					stmt.close();
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, data.getItemGuid());
					rs = stmt.executeQuery();
					rs.next();
					
					double multiplier = Double.parseDouble(rs.getString("MULTIPLIER"));
					if(data.getCurrentUOM().equalsIgnoreCase(rs.getString("BILL_UOM"))){
					//convert currentArea handQty, criticalLevel, parLevel, UOM to UOI
						newHandQty = newHandQty*multiplier;
						data.setParLevel(Integer.toString((int)Math.ceil(newHandQty*multiplier)));
						data.setCriticalLevel(Integer.toString((int)Math.ceil(newHandQty*multiplier)));
						
						if(newHandQty < Double.parseDouble(data.getTransferQuantity())){
							data.setTransferQuantity(Integer.toString((int)Math.ceil(newHandQty)));
							newHandQty = 0;
						}						
					}else{													
					//convert transferQty
						data.setCurrentUOM(rs.getString("BILL_UOM"));
						data.setTransferQuantity(Integer.toString((int)Math.ceil(Double.parseDouble(data.getTransferQuantity())*multiplier)));
						if(newHandQty >= Double.parseDouble(data.getTransferQuantity())){
							newHandQty = newHandQty - Double.parseDouble(data.getTransferQuantity()); 
						}else{
							data.setTransferQuantity(Integer.toString((int)Math.ceil(newHandQty)));
							newHandQty = 0;
						}			
					}
				}
				stmt.close();			
				stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CRITICAL_LEVEL = ?, PAR_LEVEL = ?, UOM = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
				stmt.setString(1, Integer.toString((int)Math.ceil(newHandQty)));
				stmt.setString(2, data.getCriticalLevel());
				stmt.setString(3, data.getParLevel());
				stmt.setString(4, data.getCurrentUOM());
				stmt.setTimestamp(5, todayStamp);
				stmt.setString(6, TRANSFERED_TO + transferArea);				
				stmt.setString(7, customer);
				stmt.setString(8, currentArea);
				stmt.setString(9, data.getItemGuid());
				stmt.executeUpdate();								
			}

			stmt.close();
			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_STOCK WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
			stmt.setString(1, customer);
			stmt.setString(2, transferArea);
			stmt.setString(3, data.getItemGuid());
			rs = stmt.executeQuery();				
			
			if(rs.next()){
				if(data.getCurrentUOM().equalsIgnoreCase(rs.getString("UOM"))){
					double handQty = Double.parseDouble(rs.getString("ON_HAND_QUANTITY")) + Double.parseDouble(data.getTransferQuantity()); 
					stmt.close();
					
					stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
					stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
					stmt.setTimestamp(2, todayStamp);
					stmt.setString(3, TRANSFERED_FROM + currentArea);						
					stmt.setString(4, customer);
					stmt.setString(5, transferArea);
					stmt.setString(6, data.getItemGuid());
					stmt.executeUpdate();
					stmt.close();
				}else{
					double handQty = Double.parseDouble(rs.getString("ON_HAND_QUANTITY"));
					double par = Double.parseDouble(rs.getString("PAR_LEVEL"));
					double reorder = Double.parseDouble(rs.getString("CRITICAL_LEVEL"));
					stmt.close();
					stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
					stmt.setString(1, customer);
					stmt.setString(2, data.getItemGuid());
					rs = stmt.executeQuery();
					rs.next();
					
					double multiplier = Double.parseDouble(rs.getString("MULTIPLIER"));
					if(data.getCurrentUOM().equalsIgnoreCase(rs.getString("BILL_UOM"))){
					//convert transferArea handQty, criticalLevel, parLevel, UOM to UOI; add transferQty
						handQty = handQty*multiplier + Double.parseDouble(data.getTransferQuantity());
						par = par*multiplier;
						reorder = reorder*multiplier;
						
						stmt.close();
						stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, PAR_LEVEL = ?, CRITICAL_LEVEL = ?, UOM = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
						stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
						stmt.setString(2, Integer.toString((int)Math.ceil(par)));
						stmt.setString(3, Integer.toString((int)Math.ceil(reorder)));
						stmt.setString(4, data.getCurrentUOM());
						stmt.setTimestamp(5, todayStamp);
						stmt.setString(6, TRANSFERED_FROM + currentArea);							
						stmt.setString(7, customer);
						stmt.setString(8, transferArea);
						stmt.setString(9, data.getItemGuid());
						stmt.executeUpdate();
						stmt.close();							
					}else{							
					//convert currentArea handQty to UOI for addition
						data.setTransferQuantity(Integer.toString((int)Math.ceil(Double.parseDouble(data.getTransferQuantity())*multiplier)));
						handQty = handQty + Double.parseDouble(data.getTransferQuantity());
						
						stmt.close();
						stmt = conn.prepareStatement("UPDATE PARSCAN_STOCK SET ON_HAND_QUANTITY = ?, CHANGE_TIMESTAMP = ?, CHANGE_ACTION = ? WHERE CUSTOMER = ? AND PAR_AREA = ? AND ITEM_GUID = ?");
						stmt.setString(1, Integer.toString((int)Math.ceil(handQty)));
						stmt.setTimestamp(2, todayStamp);
						stmt.setString(3, TRANSFERED_FROM + currentArea);
						stmt.setString(4, customer);
						stmt.setString(5, transferArea);
						stmt.setString(6, data.getItemGuid());
						stmt.executeUpdate();
						stmt.close();							
					}
				}										
			}else{
				stmt.close();
				stmt = conn.prepareStatement("SELECT * FROM PARSCAN_ITEM WHERE CUSTOMER = ? AND ID = ?");
				stmt.setString(1, customer);
				stmt.setString(2, data.getItemGuid());
				rs = stmt.executeQuery();
				rs.next();
				data.setItemID(rs.getString("ITEM_ID"));
				stmt.close();
				data.setOnHandQuantity(data.getTransferQuantity());
				data.setScanned("N");
				data.setUpdateAction(TRANSFERED_FROM + currentArea);
				addList.add(data);				
				addParAreaProducts(customer, transferArea, addList, true);
				addList.clear();				
			}	
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during transferServerProducts: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	/**
	 * Business Method.
	 * Returns customer's ship to list
	 */
	public List getCustomerShipTo(String customer) throws ParScanResidentsEJBException{
		List shipToList = new ArrayList();
		
		try{
			InitialContext	ic = new InitialContext();
				
			CustomerSearchSSLocalHome custLocalHome = (CustomerSearchSSLocalHome) ic.lookup("localejbs/medline.com/com.medline.srp.CustomersEAR/CustomerSearchSSBean");
			CustomerSearchSSLocal custLocal = custLocalHome.create();			
					
			List shipToListData = null;		
			shipToListData = custLocal.customerFunctions(customer);
								
			for(Iterator itor = shipToListData.iterator();itor.hasNext();){
				CustomerFunctionsBean data = (CustomerFunctionsBean) itor.next();
				Customer shipTo = new Customer();
				
				if(data.getDescription().equalsIgnoreCase("Ship-to party")){	
					shipTo.setCustomerNumber(data.getCustomerNumber());
					shipTo.setCustomerName(data.getCustomerName());
					shipToList.add(shipTo);					
				}		
			}
		}			catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getCustomerShip-to: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
		
		return shipToList;
	}		
	
	/**
	 * Business Method.
	 */
	public ParScanPropertyBean getProperties()throws ParScanResidentsEJBException {
		ParScanPropertyBean prop = new ParScanPropertyBean();
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT * FROM PARSCAN_PROPERTIES WHERE USER_ID = ?");
			stmt.setString(1, user);

			ResultSet rs = stmt.executeQuery();

			while (rs.next()) {
				prop.setItemMarkup(rs.getString("PRICE_MARKUP"));
				prop.setExportARSystem(rs.getString("AR_SYSTEM"));
				prop.setExportARCode(rs.getString("AR_CODE"));
				prop.setExportDate(rs.getString("EXPORT_DATE"));				
				break;
			}
			stmt.close();

			return prop;
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during getProperties: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 */
	public void updateProperties(ParScanPropertyBean property)throws ParScanResidentsEJBException {
		List propList = new ArrayList();
		PreparedStatement stmt;
		Connection conn = null;

		try {
			String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
			conn = openConnection();

			stmt = conn.prepareStatement("SELECT COUNT(*) as CNT FROM PARSCAN_PROPERTIES WHERE USER_ID = ?");
			stmt.setString(1, user);

			ResultSet rs = stmt.executeQuery();
			rs.next();
			if (rs.getInt("CNT") > 0) {
				stmt.close();
				
				if(!property.getItemMarkup().equalsIgnoreCase("")){
					stmt = conn.prepareStatement("UPDATE PARSCAN_PROPERTIES SET PRICE_MARKUP = ? WHERE USER_ID = ?");
					stmt.setString(1, property.getItemMarkup());
					stmt.setString(2, user);
					stmt.executeUpdate();
					stmt.close();
				}
				if(!property.getExportARSystem().equalsIgnoreCase("")){
					stmt = conn.prepareStatement("UPDATE PARSCAN_PROPERTIES SET AR_SYSTEM = ? WHERE USER_ID = ?");
					stmt.setString(1, property.getExportARSystem());
					stmt.setString(2, user);
					stmt.executeUpdate();
					stmt.close();
				}				
				if(!property.getExportARCode().equalsIgnoreCase("")){
					stmt = conn.prepareStatement("UPDATE PARSCAN_PROPERTIES SET AR_CODE = ? WHERE USER_ID = ?");
					stmt.setString(1, property.getExportARCode());
					stmt.setString(2, user);
					stmt.executeUpdate();
					stmt.close();
				}
				if(!property.getExportDate().equalsIgnoreCase("")){
					stmt = conn.prepareStatement("UPDATE PARSCAN_PROPERTIES SET EXPORT_DATE = ? WHERE USER_ID = ?");
					stmt.setString(1, property.getExportDate());
					stmt.setString(2, user);
					stmt.executeUpdate();
					stmt.close();
				}												
			}else{
				stmt.close();
				
				stmt = conn.prepareStatement("INSERT INTO PARSCAN_PROPERTIES (ID,USER_ID,PRICE_MARKUP,AR_SYSTEM,AR_CODE,EXPORT_DATE) VALUES (?,?,?,?,?,?)");
				UID uid = new UID();

				stmt.setString(1, uid.toString());				
				stmt.setString(2, user);
				stmt.setString(3, emptyString(property.getItemMarkup()));
				stmt.setString(4, emptyString(property.getExportARSystem()));
				stmt.setString(5, emptyString(property.getExportARCode()));
				stmt.setString(6, emptyString(property.getExportDate()));
				stmt.executeUpdate();
				stmt.close();
			}
		} catch (Exception ex) {
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT(
				"Error during updateProperties: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}
	}	
	
	/**
	 * Business Method.
	 * Adds a new par areas for a customer
	 */
	public void editPONumber(String customer, String po, String newPO, String free) throws ParScanResidentsEJBException{			
			Connection conn = null;
			
			try
			{
				String user = UMFactory.getAuthenticator().getLoggedInUser().getUniqueName().toUpperCase();
				conn = openConnection();
				PreparedStatement stmt;

				if(free.equalsIgnoreCase("X")){
					stmt = conn.prepareStatement("UPDATE PARSCAN_FREE_ORDER SET PO_NUMBER = ? WHERE CUSTOMER = ? AND PO_NUMBER = ?");
					stmt.setString(1, newPO);
					stmt.setString(2, customer);
					stmt.setString(3, po);
					stmt.executeUpdate();
					stmt.close();					
				}else{
					stmt = conn.prepareStatement("UPDATE PARSCAN_ORDER SET PO_NUMBER = ? WHERE CUSTOMER = ? AND PO_NUMBER = ?");
					stmt.setString(1, newPO);
					stmt.setString(2, customer);
					stmt.setString(3, po);
					stmt.executeUpdate();
					stmt.executeUpdate();
					stmt.close();					
				}
			}
			catch (Exception ex)
			{
				StringWriter stringWriter = new StringWriter();
				ex.printStackTrace(new PrintWriter(stringWriter));
				ParScanResidentsSSBean.logger.errorT("Error during editPONumber: " + stringWriter.toString());
				throw new ParScanResidentsEJBException(ex);
			}	
		finally{
			try {
				if (conn != null)conn.close();
			} catch (SQLException e) {
				
				e.printStackTrace();
			}
		}	
	}	
	
	public void usagePDFReport(Message msg)throws ParScanResidentsEJBException{
		NumberFormat usDollarFormat = NumberFormat.getCurrencyInstance(Locale.US);
		NumberFormat usFormat = NumberFormat.getIntegerInstance(Locale.US);		
		int index;
		String temp;
		
		Document document = new Document(PageSize.LETTER.rotate(), 0,0,0,0); 		
		try {
			List itemList = new ArrayList();
			List sendList = new ArrayList();
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
			
			JSONArray jsonArray = new JSONArray(msg.getStringProperty("jsonData"));
			JSONSort JSONSt = new JSONSort(); 
			JSONSt.setDirection(true); 
			JSONSt.setField("description");
			jsonArray.sortArray(JSONSt);			
			DecimalFormat outNumberFormat = new DecimalFormat("0.00");
			
			//future price list
			for (int i = 0; i < jsonArray.length(); i++)
			{	
				JSONObject obj = jsonArray.getJSONObject(i);
				if(!obj.get("curprice").toString().equalsIgnoreCase("") && !obj.get("curcost").toString().equalsIgnoreCase("") && !obj.get("multiplier").toString().equalsIgnoreCase("")){	
					ParScanItemBean data = new ParScanItemBean();				

					data.setItemGUID(obj.get("guid").toString());
					data.setItemID(obj.get("id").toString());
					data.setDescription(obj.get("description").toString());
					data.setBillUom(obj.get("billuom").toString());
					data.setCurrentPrice(obj.get("curprice").toString());
					data.setCurrentCost(outNumberFormat.format(Double.parseDouble(obj.get("curcost").toString())/Double.parseDouble(obj.get("multiplier").toString())));
					
					sendList.add(data);
				}
			}

			itemList = getResidentUsageAnalysis(msg.getStringProperty("customer"), sendList, sdf.parse(msg.getStringProperty("from")), sdf.parse(msg.getStringProperty("to")));
			
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			Date convertedDate; 	
			
			PdfWriter.getInstance(document, out);
		
			Phrase footerPhrase = new Phrase();
			Font footerFont = new Font();
			footerFont.setSize(8);
			Chunk footerChunk = new Chunk(msg.getStringProperty("customer") + " - " + msg.getStringProperty("customercustomerName") + "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t");
			footerChunk.setFont(footerFont);
			footerPhrase.add(footerChunk);			
			HeaderFooter footer = new HeaderFooter(footerPhrase, true);
			document.setFooter(footer);		
		
			document.open();
			Font titleFont = new Font();
			Paragraph paragraph = new Paragraph();
			
			Phrase headerPhrase = new Phrase();
			PdfPTable headerTable = new PdfPTable(6);
			int widths[] = { 7, 19, 15, 19, 21, 19 }; // percentage
			headerTable.setWidths(widths);
			headerTable.setWidthPercentage(100); // percentage			
			headerTable.getDefaultCell().setBorder(0);
			headerTable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			headerTable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_TOP);				
			
			String environmentPrefix = getPortalEnvironmentPrefix();
			Image jpg = Image.getInstance(environmentPrefix + "/medline/resources/images/medline-small.gif");
			headerTable.addCell(jpg);
			
			titleFont = new Font();
			titleFont.setSize(9);			
			
			headerPhrase = new Phrase();
			Font blue = FontFactory.getFont(FontFactory.HELVETICA, 9, Font.NORMAL, new Color(0x00, 0x00, 0xFF));
			Chunk chunk = new Chunk("Medline Industries, Inc.", blue);
			headerPhrase.add(chunk);
			chunk = new Chunk("\n");
			headerPhrase.add(chunk);			
			chunk = new Chunk("www.medline.com", blue);
			headerPhrase.add(chunk);			
			headerTable.addCell(headerPhrase);
			headerTable.addCell("");
			
			headerPhrase = new Phrase();
			chunk = new Chunk("One Medline Place");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			chunk = new Chunk("\n");
			headerPhrase.add(chunk);
			chunk = new Chunk("Mundelein, IL 60060-4486");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);						
			headerTable.addCell(headerPhrase);
			headerTable.addCell("");		
			
			headerPhrase = new Phrase();
			chunk = new Chunk("Phone: 1.847.949.5500");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);	
			chunk = new Chunk("\n");
			headerPhrase.add(chunk);	
			chunk = new Chunk("Toll Free: 1.800.MEDLINE");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			headerTable.addCell(headerPhrase);
			document.add(headerTable);
			
			Phrase titlePhrase = new Phrase();
			PdfPTable titleTable = new PdfPTable(1);
			titleTable.setWidthPercentage(100); // percentage			
			titleTable.getDefaultCell().setBorder(0);
			titleTable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			titleTable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);				
			chunk = new Chunk("Current Usage for: " + msg.getStringProperty("from") + " - " + msg.getStringProperty("to"));
			titleFont.setSize(12);
			titleFont.setStyle(Font.BOLD);
			titleFont.setStyle(Font.UNDERLINE);
			chunk.setFont(titleFont);
			titlePhrase.add(chunk);
			titleTable.addCell(titlePhrase);
			document.add(titleTable);
			document.add(new Paragraph("\n"));
			
			PdfPTable table = new PdfPTable(5);
			table.getDefaultCell().setBorder(0);
			table.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			table.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);			

			headerPhrase = new Phrase();
			chunk = new Chunk("Customer Number: ");
			titleFont = new Font();
			titleFont.setSize(9);
			titleFont.setStyle(Font.BOLD);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			table.addCell(headerPhrase);
			
			headerPhrase = new Phrase();
			chunk = new Chunk(msg.getStringProperty("customer"));
			titleFont = new Font();
			titleFont.setSize(9);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);			
			table.addCell(headerPhrase);
			table.addCell("");
			
			headerPhrase = new Phrase();
			chunk = new Chunk("Customer Name: ");
			titleFont = new Font();
			titleFont.setSize(9);
			titleFont.setStyle(Font.BOLD);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			table.addCell(headerPhrase);
			
			headerPhrase = new Phrase();
			chunk = new Chunk(msg.getStringProperty("customerName"));
			titleFont = new Font();
			titleFont.setSize(9);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);			
			table.addCell(headerPhrase);			
			
			document.add(table);					
			document.add(new Paragraph("\n"));
			
			PdfPTable datatable;			
			datatable = new PdfPTable(9);			
			datatable.getDefaultCell().setBorder(0);
			datatable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);
			int headerwidths[] = { 15, 20, 15, 5, 5, 10, 10, 10, 10}; // percentage
			datatable.setWidths(headerwidths);
			datatable.setWidthPercentage(98); // percentage			
			Font headerFont = new Font();
			headerFont.setSize(9);
			headerFont.setStyle(Font.BOLD);
		
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			Phrase phrase = new Phrase();
			chunk = new Chunk("Product ID");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			phrase = new Phrase();
			chunk = new Chunk("Product Description");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);			
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			phrase = new Phrase();
			chunk = new Chunk(msg.getStringProperty("title"));
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);				
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("Usage");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);				
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("UOM");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);		
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("Price");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);		
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("Total Charged");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);		
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("Cost");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);		
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("Total Cost");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);																	
				
			document.add(datatable);	
			
			datatable = new PdfPTable(9);			
			datatable.getDefaultCell().setBorder(0);
			datatable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);
			int headerItemwidths[] = new int[]{ 15, 20, 15, 5, 5, 10, 10, 10, 10};		
			datatable.setWidths(headerItemwidths);
			datatable.setWidthPercentage(98); // percentage			
			Font lineFont = new Font();
			lineFont.setSize(7);
			lineFont.setStyle(Font.NORMAL);
			
			for(Iterator itor = itemList.iterator();itor.hasNext();){
				ParScanItemBean data = (ParScanItemBean) itor.next();				
												
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
				phrase = new Phrase();
				chunk = new Chunk(data.getItemID());
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);				
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
				phrase = new Phrase();
				chunk = new Chunk(data.getDescription());
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);							
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
				phrase = new Phrase();
				chunk = new Chunk(data.getResidentName());
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				phrase = new Phrase();
				chunk = new Chunk(data.getTotalUsage());
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);				
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				phrase = new Phrase();
				chunk = new Chunk(data.getBillUom());
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				phrase = new Phrase();
				chunk = new Chunk(usDollarFormat.format(Double.parseDouble(data.getCurrentPrice())));
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);			
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				phrase = new Phrase();
				chunk = new Chunk(usDollarFormat.format(Double.parseDouble(data.getTotalCharged())));
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);			
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				phrase = new Phrase();
				chunk = new Chunk(usDollarFormat.format(Double.parseDouble(data.getCurrentCost())));
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);											
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				phrase = new Phrase();
				chunk = new Chunk(usDollarFormat.format(Double.parseDouble(data.getTotalCost())));
				chunk.setFont(lineFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);				
			}
			document.add(datatable);						
				
			document.close();
					
			//CONVERT OUTPUTSTREAM TO INPUTSTREAM AND SAVE ON SERVER
			String fileName = "";
			DateFormat dateForm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			String sysdate = dateForm.format(date);
			sysdate = sysdate.replaceAll("/","");
			sysdate = sysdate.replaceAll(" ","_");
			sysdate = sysdate.replaceAll(":","");
								
			fileName = "Current Usage_" + sysdate + ".pdf";			
			
			InputStream uploadedStream = new ByteArrayInputStream(out.toByteArray());
			com.sapportals.wcm.repository.IResource aResource = null;
			IResourceContext context = KMUtils.getServiceUserResourceContext();
			// create a RID to handle a resource
			RID bRid = new RID("/fileupload/InventoryMaintenance");
			try {
				// get an instance of a resource factory
				//IResourceFactory aResourceFactory = ResourceFactory.getInstance();
				// retrieve a collection with context
				ICollection parent = (ICollection) ResourceFactory.getInstance().getResource(bRid, context);
				//aCollection = (ICollection) aResourceFactory.getResource(aRid, rContext);
				// create a new content for this resource
				IContent bContent = new Content(uploadedStream, "application/pdf", -1L);
				
				if (parent != null) {
					// create the resource
					aResource = parent.createResource(fileName, null, bContent);
					//this.status="SUCCESS";
				}
			} catch (AccessDeniedException ex) {					
			} catch (com.sapportals.wcm.repository.ResourceException ex) {
			}
			
			//SEND EMAIL
			com.sapportals.portal.security.usermanagement.IUser user =
			(com.sapportals.portal.security.usermanagement.IUser) WPUMFactory.getServiceUserFactory().getServiceUser("cmadmin_service");
			
			
			IApplicationEmail iemail = (IApplicationEmail) PortalRuntime.getRuntimeResources().getService(IApplicationEmail.KEY);
			IApplicationEmailItem email = iemail.createEmailItem();
			
			email.setFrom("parscan_no_reply@medline.com");
			email.setTo(msg.getStringProperty("email"));
			email.setSubject("ParScan Usage Report Attached");
			email.setContent("The ParScan Current Usage Report you requested is attached. DO NOT REPLY TO THIS EMAIL.");
			email.addAttachment(getDocument(fileName));
			iemail.sendEmail(user, email);
		} catch (DocumentException e) {
		
			e.printStackTrace();
		} catch (MalformedURLException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (ParScanResidentsEJBException e) {
			
			e.printStackTrace();
		} catch (Exception ex){
			try {
				sendErrorEmail(msg.getStringProperty("email"));
			}catch (JMSException e1) {
				
				e1.printStackTrace();
			}			
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during TransactionPDFReport: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);			
		}	
	}
	
	public void transactionPDFReport(ParScanReportMessage msg)throws ParScanResidentsEJBException{
		int index;
		String temp;
				
		Document document = new Document(PageSize.LETTER, 0,0,0,0); 		
		try {	
			ByteArrayOutputStream out = new ByteArrayOutputStream();		
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
			NumberFormat usDollarFormat = NumberFormat.getCurrencyInstance(Locale.US);
	
			String fromToDateTitle = dateFormat.format( msg.getFromDate() ) + " - " + dateFormat.format( msg.getToDate() );
			
			Date convertedDate; 
			String title = ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()) ?
									"Transaction History Log ("+ fromToDateTitle +"): " + msg.getParArea() :
									"Par Area Transfers (" +  fromToDateTitle +"): " + msg.getParArea();				
			
			PdfWriter.getInstance(document, out);
		
			Phrase footerPhrase = new Phrase();
			Font footerFont = new Font();
			footerFont.setSize(8);
			Chunk footerChunk = new Chunk(msg.getCustomerNumber() + " - " + msg.getCustomerName() + "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t");
			footerChunk.setFont(footerFont);
			footerPhrase.add(footerChunk);			
			HeaderFooter footer = new HeaderFooter(footerPhrase, true);
			document.setFooter(footer);		
		
			document.open();
			Font titleFont = new Font();
			Paragraph paragraph = new Paragraph();
			
			Phrase headerPhrase = new Phrase();
			PdfPTable headerTable = new PdfPTable(6);
			int widths[] = { 7, 19, 15, 19, 21, 19 }; // percentage
			headerTable.setWidths(widths);
			headerTable.setWidthPercentage(100); // percentage			
			headerTable.getDefaultCell().setBorder(0);
			headerTable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			headerTable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_TOP);				
			
			String environmentPrefix = getPortalEnvironmentPrefix();
			Image jpg = Image.getInstance(environmentPrefix + "/medline/resources/images/medline-small.gif");
			headerTable.addCell(jpg);
			
			titleFont = new Font();
			titleFont.setSize(9);			
			
			headerPhrase = new Phrase();
			Font blue = FontFactory.getFont(FontFactory.HELVETICA, 9, Font.NORMAL, new Color(0x00, 0x00, 0xFF));
			Chunk chunk = new Chunk("Medline Industries, Inc.", blue);
			headerPhrase.add(chunk);
			chunk = new Chunk("\n");
			headerPhrase.add(chunk);			
			chunk = new Chunk("www.medline.com", blue);
			headerPhrase.add(chunk);			
			headerTable.addCell(headerPhrase);
			headerTable.addCell("");
			
			headerPhrase = new Phrase();
			chunk = new Chunk("One Medline Place");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			chunk = new Chunk("\n");
			headerPhrase.add(chunk);
			chunk = new Chunk("Mundelein, IL 60060-4486");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);						
			headerTable.addCell(headerPhrase);
			headerTable.addCell("");		
			
			headerPhrase = new Phrase();
			chunk = new Chunk("Phone: 1.847.949.5500");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);	
			chunk = new Chunk("\n");
			headerPhrase.add(chunk);	
			chunk = new Chunk("Toll Free: 1.800.MEDLINE");
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			headerTable.addCell(headerPhrase);
			document.add(headerTable);
			
			Phrase titlePhrase = new Phrase();
			PdfPTable titleTable = new PdfPTable(1);
			titleTable.setWidthPercentage(100); // percentage			
			titleTable.getDefaultCell().setBorder(0);
			titleTable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			titleTable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);				
			chunk = new Chunk(title);
			titleFont.setSize(12);
			titleFont.setStyle(Font.BOLD);
			titleFont.setStyle(Font.UNDERLINE);
			chunk.setFont(titleFont);
			titlePhrase.add(chunk);
			titleTable.addCell(titlePhrase);
			document.add(titleTable);
			document.add(new Paragraph("\n"));
			
			PdfPTable table = new PdfPTable(5);
			table.getDefaultCell().setBorder(0);
			table.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			table.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);			

			headerPhrase = new Phrase();
			chunk = new Chunk("Customer Number: ");
			titleFont = new Font();
			titleFont.setSize(9);
			titleFont.setStyle(Font.BOLD);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			table.addCell(headerPhrase);
			
			headerPhrase = new Phrase();
			chunk = new Chunk(msg.getCustomerNumber());
			titleFont = new Font();
			titleFont.setSize(9);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);			
			table.addCell(headerPhrase);
			table.addCell("");
			
			headerPhrase = new Phrase();
			chunk = new Chunk("Customer Name: ");
			titleFont = new Font();
			titleFont.setSize(9);
			titleFont.setStyle(Font.BOLD);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);
			table.addCell(headerPhrase);
			
			headerPhrase = new Phrase();
			chunk = new Chunk(msg.getCustomerName());
			titleFont = new Font();
			titleFont.setSize(9);
			chunk.setFont(titleFont);
			headerPhrase.add(chunk);			
			table.addCell(headerPhrase);			
			
			document.add(table);					

			PdfPTable datatable;			
			datatable = new PdfPTable(2);			
			datatable.getDefaultCell().setBorder(1);
			datatable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);
			int headerwidths[] = { 20, 80}; // percentage
			datatable.setWidths(headerwidths);
			datatable.setWidthPercentage(90); // percentage			
			Font headerFont = new Font();
			headerFont.setSize(9);
			headerFont.setStyle(Font.BOLD);
		
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			Phrase phrase = new Phrase();
			chunk = new Chunk("Product ID");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			phrase = new Phrase();
			chunk = new Chunk("Product Description");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);			
			document.add(datatable);

			int cols = ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()) ? 7 : 6;
			datatable = new PdfPTable(cols);			
			datatable.getDefaultCell().setBorder(2);
			datatable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);
			int headerItemwidths7[] = new int[]{ 5, 25, 30, 10, 10, 10, 10}; // percentage
			int headerItemwidths6[] = new int[]{ 5, 25, 30, 13, 13, 14}; // percentage		
			datatable.setWidths(ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()) ? headerItemwidths7 : headerItemwidths6);
			datatable.setWidthPercentage(90); // percentage			
			headerFont = new Font();
			headerFont.setSize(9);
			headerFont.setStyle(Font.BOLD);
			double totalCost = 0;
														
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			phrase = new Phrase();
			chunk = new Chunk("");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			phrase = new Phrase();
			chunk = new Chunk("Date/Time");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
			phrase = new Phrase();
			chunk = new Chunk("Stock Update Action");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);						
			document.add(datatable);	
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("UOM");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);						
			document.add(datatable);	
			headerFont = new Font();
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("Quantity");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);						
			document.add(datatable);
			if(ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()))
			{
				datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
				phrase = new Phrase();
				chunk = new Chunk("Loss/Gain");
				chunk.setFont(headerFont);					
				phrase.add(chunk);
				datatable.addCell(phrase);						
				document.add(datatable);
			}	
			datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);
			phrase = new Phrase();
			chunk = new Chunk("System Quantity");
			chunk.setFont(headerFont);					
			phrase.add(chunk);
			datatable.addCell(phrase);						
			document.add(datatable);	
						
			headerFont.setSize(8);
			headerFont.setStyle(Font.BOLD);
			headerFont.setStyle(Font.ITALIC);					
			Font lineFont = new Font();
			lineFont.setSize(7);
			lineFont.setStyle(Font.NORMAL);

			List itemList = getParAreaStock(msg.getCustomerNumber(), msg.getParArea());			
			for(Iterator itor = itemList.iterator();itor.hasNext();){
				ParScanStockBean data = (ParScanStockBean) itor.next();
				List itemData = getStockLog(msg, data.getItemGuid());
				
				if(itemData.size() > 0){
					datatable = new PdfPTable(2);			
					datatable.getDefaultCell().setBorder(0);
					datatable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);
					datatable.setWidths(headerwidths);
					datatable.setWidthPercentage(90); // percentage		

					phrase = new Phrase();
					datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
					chunk = new Chunk(data.getItemID());					
					chunk.setFont(headerFont);
					phrase.add(chunk);						
					datatable.addCell(phrase);
					phrase = new Phrase();
					datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);
					chunk = new Chunk(data.getItemDescription());					
					chunk.setFont(headerFont);
					phrase.add(chunk);						
					datatable.addCell(phrase);
					document.add(datatable);
					datatable = new PdfPTable(cols);			
					datatable.getDefaultCell().setBorder(0);
					datatable.getDefaultCell().setVerticalAlignment(PdfPCell.ALIGN_MIDDLE);
					datatable.setWidths(ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()) ? headerItemwidths7 : headerItemwidths6);
					datatable.setWidthPercentage(90); // percentage	
					logger.debugT("ID/Descr");
														
					int count = 0;
					int sum = 0;
					int curInv = 0;
					//int deviceCount = 0;
					double cost = 0;
					int lossgain = 0;
					for(Iterator itemItor = itemData.iterator();itemItor.hasNext();){
						if (count % 2 == 1) {
							datatable.getDefaultCell().setGrayFill(0.9f);						
						}							
						ParScanStockBean item = (ParScanStockBean) itemItor.next();
						cost = Double.parseDouble(item.getCost());
												
						phrase = new Phrase();
						chunk = new Chunk("");					
						phrase.add(chunk);
						datatable.addCell(phrase);	
						phrase = new Phrase();
						datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);						
						chunk = new Chunk(item.getUpdateDate());					
						chunk.setFont(lineFont);
						phrase.add(chunk);						
						datatable.addCell(phrase);									
						phrase = new Phrase();
						datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_LEFT);						
						chunk = new Chunk(item.getUpdateAction());					
						chunk.setFont(lineFont);
						phrase.add(chunk);						
						datatable.addCell(phrase);				
						phrase = new Phrase();
						datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
						chunk = new Chunk(item.getCurrentUOM());					
						chunk.setFont(lineFont);
						phrase.add(chunk);						
						datatable.addCell(phrase);		
						phrase = new Phrase();					
						datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
						chunk = new Chunk(item.getUpdateQuantity());					
						chunk.setFont(lineFont);
						phrase.add(chunk);						
						datatable.addCell(phrase);					
						if(count == 0){
							curInv = Integer.parseInt(item.getUpdateQuantity());
							if(ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()))
							{
								phrase = new Phrase();					
								datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
								chunk = new Chunk("");					
								chunk.setFont(lineFont);
								phrase.add(chunk);						
								datatable.addCell(phrase);	
							}
								
							phrase = new Phrase();					
							datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
							chunk = new Chunk(Integer.toString(curInv));
							chunk.setFont(lineFont);
							phrase.add(chunk);						
							datatable.addCell(phrase);								
						}else{
							if(item.getUpdateAction().equalsIgnoreCase("Inventory Count") || item.getUpdateAction().equalsIgnoreCase("Online Count Change")){
								lossgain = Integer.parseInt(item.getUpdateQuantity()) - curInv;
								sum = sum + lossgain;
								curInv = Integer.parseInt(item.getUpdateQuantity());
								if(ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()))
								{
									phrase = new Phrase();					
									datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
									chunk = new Chunk(Integer.toString(lossgain));					
									chunk.setFont(lineFont);
									phrase.add(chunk);						
									datatable.addCell(phrase);
								}											
								phrase = new Phrase();					
								datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
								chunk = new Chunk(Integer.toString(curInv));		
								chunk.setFont(lineFont);
								phrase.add(chunk);						
								datatable.addCell(phrase);						
							}else{
								if(ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()))
								{
									phrase = new Phrase();					
									datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
									chunk = new Chunk("");					
									chunk.setFont(lineFont);
									phrase.add(chunk);						
									datatable.addCell(phrase);
								}
								curInv = curInv + Integer.parseInt(item.getUpdateQuantity());	

								phrase = new Phrase();					
								datatable.getDefaultCell().setHorizontalAlignment(PdfPCell.ALIGN_CENTER);						
								chunk = new Chunk(Integer.toString(curInv));	
								chunk.setFont(lineFont);
								phrase.add(chunk);						
								datatable.addCell(phrase);																
							}
						}

						if (count % 2 == 1) {
							datatable.getDefaultCell().setGrayFill(1);
						}
						count++;						
					}
					document.add(datatable);
					if(ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()))
					{														
						chunk = new Chunk("Net Loss/Gain: " + Integer.toString(sum) + " Cost: " + usDollarFormat.format(sum*cost));
						Font totalFont = new Font();
						totalFont.setSize(9);
						totalFont.setStyle(Font.BOLD);
						totalFont.setStyle(Font.UNDERLINE);
						chunk.setFont(totalFont);
						paragraph = new Paragraph();
						paragraph.add(chunk);
						paragraph.setAlignment(Element.ALIGN_RIGHT);
						paragraph.setIndentationRight(25f);
						document.add(paragraph);
					}				
					itemData.clear();	
				}			
			}
				
			document.close();
			//CONVERT OUTPUTSTREAM TO INPUTSTREAM AND SAVE ON SERVER
			String fileName = "";
			DateFormat dateForm = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			String sysdate = dateForm.format(date);
			sysdate = sysdate.replaceAll("/","");
			sysdate = sysdate.replaceAll(" ","_");
			sysdate = sysdate.replaceAll(":","");
								
			fileName = ParScanReportMessage.TRANSACTION_REPORT.equalsIgnoreCase(msg.getReportType()) ? 
						"Transaction History Log_" + sysdate + ".pdf" : "Par Area Transfers_" + sysdate + ".pdf";		
			
			InputStream uploadedStream = new ByteArrayInputStream(out.toByteArray());
			com.sapportals.wcm.repository.IResource aResource = null;
			IResourceContext context = KMUtils.getServiceUserResourceContext();
			// create a RID to handle a resource
			RID bRid = new RID("/fileupload/InventoryMaintenance");
			try {
				// get an instance of a resource factory
				//IResourceFactory aResourceFactory = ResourceFactory.getInstance();
				// retrieve a collection with context
				ICollection parent = (ICollection) ResourceFactory.getInstance().getResource(bRid, context);
				//aCollection = (ICollection) aResourceFactory.getResource(aRid, rContext);
				// create a new content for this resource
				IContent bContent = new Content(uploadedStream, "application/pdf", -1L);
				
				if (parent != null) {
					// create the resource
					aResource = parent.createResource(fileName, null, bContent);
					//this.status="SUCCESS";
				}
			} catch (AccessDeniedException ex) {					
			} catch (com.sapportals.wcm.repository.ResourceException ex) {
			}
			
			//SEND EMAIL
			com.sapportals.portal.security.usermanagement.IUser user =
			(com.sapportals.portal.security.usermanagement.IUser) WPUMFactory.getServiceUserFactory().getServiceUser("cmadmin_service");
			
			
			IApplicationEmail iemail = (IApplicationEmail) PortalRuntime.getRuntimeResources().getService(IApplicationEmail.KEY);
			IApplicationEmailItem email = iemail.createEmailItem();
			
			email.setFrom("parscan_no_reply@medline.com");
			email.setTo(msg.getEmail());
			email.setSubject("ParScan Transaction Report Attached");
			email.setContent("The ParScan Transaction History Report you requested is attached. DO NOT REPLY TO THIS EMAIL.");
			email.addAttachment(getDocument(fileName));
			iemail.sendEmail(user, email);
		} catch (DocumentException e) {
		
			e.printStackTrace();
		} catch (MalformedURLException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		} catch (ParScanResidentsEJBException e) {
			
			e.printStackTrace();
		} catch (Exception ex){
			sendErrorEmail(msg.getEmail());			
			
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during TransactionPDFReport: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);			
		} 	
	}	
	
	private void uploadReqFiletoFTP(String username, String password, String fileName, InputStream input){
		// get an ftpClient object  
		FTPClient ftpClient = new FTPClient();    
  
		try {  
		 // pass directory path on server to connect  
		 logger.debugT("connect to ftp.medline.com");
		 ftpClient.setDefaultPort(21);
		 ftpClient.setDefaultTimeout(5000);
		 ftpClient.connect("192.168.53.7");  
		 logger.debugT("connected to ftp.medline.com");
		 // pass username and password, returned true if authentication is  
		 // successful  
		 boolean login = ftpClient.login(username, password);  
  
		 if (login) {  
		  ParScanResidentsSSBean.logger.debugT("Connection established...");
		    
		  //inputStream = new FileInputStream("files/fileToUpload.txt");  
  
		  boolean uploaded = ftpClient.storeFile(fileName,input);  
		  if (uploaded) {  
			ParScanResidentsSSBean.logger.debugT("File uploaded successfully !");  
		  } else {  
			ParScanResidentsSSBean.logger.debugT("Error in uploading file !");  
		  }  
  
		  // logout the user, returned true if logout successfully  
		  boolean logout = ftpClient.logout();  
		  if (logout) {  
			ParScanResidentsSSBean.logger.debugT("Connection close...");  
		  }  
		 } else {  
			ParScanResidentsSSBean.logger.debugT("Connection fail...");  
		 }  
  
		} catch (SocketException e) {  
		 e.printStackTrace();  
		} catch (IOException e) {  
		 e.printStackTrace();  
		} finally {  
		 try {  
		  ftpClient.disconnect();  
		 } catch (IOException e) {  
		  e.printStackTrace();  
		 }  
		}  	
	}
	
	private void sendErrorEmail(String emailTo)throws ParScanResidentsEJBException{
		try{
			//SEND EMAIL
			com.sapportals.portal.security.usermanagement.IUser user =
			(com.sapportals.portal.security.usermanagement.IUser) WPUMFactory.getServiceUserFactory().getServiceUser("cmadmin_service");
			
			
			IApplicationEmail iemail = (IApplicationEmail) PortalRuntime.getRuntimeResources().getService(IApplicationEmail.KEY);
			IApplicationEmailItem email = iemail.createEmailItem();
			
			email.setFrom("parscan_no_reply@medline.com");
			email.setTo(emailTo);
			email.setSubject("ParScan Report ERROR");
			email.setContent("The ParScan report you requested generated an error. Please contact the Medline ParScan support team at 1-800-950-0292. DO NOT REPLY TO THIS EMAIL.");
			iemail.sendEmail(user, email);		
		}catch (ApplicationEmailException e) {
			
			e.printStackTrace();
		}catch (Exception ex){
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during sendErrorEmail: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);			
		}
	}
	
	private String getNewResidentCRMGUID(String customer, String id)throws ParScanResidentsEJBException{
		IConnection connection = null;
		String GUID  = "";
	
		try{
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "ZV_PATIENT_SEARCH");

			IFunctionsMetaData functionsMetaData =
				connection.getFunctionsMetaData();
			IFunction function =
				functionsMetaData.getFunction("ZV_PATIENT_SEARCH");

			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams =
				recordFactory.createMappedRecord("input");
			IStructureFactory structureFactory =
				interaction.retrieveStructureFactory();

			importParams.put("I_SOLDTO", customer);
			importParams.put("I_PATIENT_ID", id);
			importParams.put("I_PNAME", "RESIDENT NEW");

			MappedRecord exportParams =
				(MappedRecord) interaction.execute(
					interactionSpec,
					importParams);
			IRecordSet residents =
				(IRecordSet) exportParams.get("T_PATIENT_DATA");

			while (residents.next()) {
				GUID = residents.getString("GUID");
			}
			
			return GUID;
		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getNewResidentCRMGUID: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}		
		finally
		{
			try
			{
				if (connection != null)
					connection.close();
			}
			catch (ResourceException ex)
			{
			}
		}	
	}
	
	private String getNAValue(String string)
	{
		string = getValue(string);
		return (StringUtils.isEmpty(string)) ? "N/A" : string;
	}		
	
	private String getValue(String string)
	{

		return (string != null) ? StringUtils.escapeChars(string) : "";
	}	
	
	private String getSalesUOM(UnitOfMeasure measure)
	{
		return (measure != null) ? getValue(measure.getUnitOfMeasure()) : "";
	}	
	
	private String determineStatus(String onHandQty, String parQty){
		String status= "";
		double onHandQ = Double.parseDouble(onHandQty);
		double parQ = Double.parseDouble(parQty);
		
		if(parQ != 0){
			double percent = onHandQ/parQ;
			if(percent > .7){
				status = "High";
			}
			else if(percent > .4 && percent <= .7){
				status = "Medium";
			}
			else if (percent <= .4){
				status = "Low";
			}
		}else{
			status = "Low";
		}
		
		return status;
	}
	
	private double getConversion(String uom, String product)throws ParScanResidentsEJBException{
		try{
			List alternateUOM = getMedlineAlternateUOM(product);
			double conversion = 0;
						
			for(Iterator itor = alternateUOM.iterator();itor.hasNext();){
				ParScanUOMBean data = (ParScanUOMBean) itor.next();
				if(uom.equalsIgnoreCase(data.getAltUoM())){
					conversion = Double.parseDouble(data.getUmrez())/Double.parseDouble(data.getUmren());
					break;
				}
			}
		
			return conversion;
		}catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getConversion: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}	
	}
	
	private List getMedlineAlternateUOM(String product) throws ParScanResidentsEJBException
	{
		IConnection connection = null;
		List uomList = new ArrayList();

		try
		{
			//get a connection
			connection = getR3Connection();

			IInteraction interaction = connection.createInteractionEx();
			IInteractionSpec interactionSpec = interaction.getInteractionSpec();
			interactionSpec.setPropertyValue("Name", "Z_GET_MARM_DATA");

			IFunctionsMetaData functionsMetaData = connection.getFunctionsMetaData();
			IFunction function = functionsMetaData.getFunction("Z_GET_MARM_DATA");


			// Input values
			RecordFactory recordFactory = interaction.getRecordFactory();
			MappedRecord importParams = recordFactory.createMappedRecord("input");
			importParams.put("IN_MATERIAL", product);			

			MappedRecord exportParams = (MappedRecord) interaction.execute(interactionSpec, importParams);
			IRecordSet uom = (IRecordSet) exportParams.get("ZMARMDATA");

			while (uom.next())
			{
				ParScanUOMBean data = new ParScanUOMBean();
				if(uom.getString("MEINH").equalsIgnoreCase("BOT")){
					data.setAltUoM("BT");
					data.setUmren("1");
					data.setUmrez("1");
				}if(uom.getString("MEINH").equalsIgnoreCase("PAA") || uom.getString("MEINH").equalsIgnoreCase("PR")){
					data.setAltUoM("PR");
					data.setUmren("1");
					data.setUmrez("1");
				}if(uom.getString("UMREN").equalsIgnoreCase("0") || uom.getString("UMREZ").equalsIgnoreCase("0")){
					data.setUmren("1");
					data.setUmrez("1");
				}else{
					data.setAltUoM(uom.getString("MEINH"));
					data.setUmren(uom.getString("UMREN"));
					data.setUmrez(uom.getString("UMREZ"));					
				}

				uomList.add(data);
				
				if (uomList.size() >= 100 )
					break;
			}

		}
		catch (Exception ex)
		{
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getMedlineAlternateUOM: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}		
		finally
		{
			try
			{
				if (connection != null)
					connection.close();
			}
			catch (ResourceException ex)
			{
			}
		}

		return uomList;
	}


	private String getMedlinePrice(String customer, String product)throws ParScanResidentsEJBException{		
		String currentPrice = "";
		
		try{
			String[] products = new String [1];	
			products[0] = product;
						
			InitialContext	ic = new InitialContext();
				
			PricingSSLocalHome priceLocalHome = (PricingSSLocalHome) ic.lookup("localejbs/medline.com/com.medline.srp.PricingEAR/PricingSSBean");
			PricingSSLocal priceLocal = priceLocalHome.create();			
					
			List prices = null;		
			prices = priceLocal.getPricingDetails(customer,new Date(),"OR", products);
								
			for(Iterator itor = prices.iterator();itor.hasNext();){
				PricingDetailsBean pricingData = (PricingDetailsBean) itor.next();				

				currentPrice = Float.toString(pricingData.getPrice());	
			}			
		
		}catch (Exception ex){
			StringWriter stringWriter = new StringWriter();
			ex.printStackTrace(new PrintWriter(stringWriter));
			ParScanResidentsSSBean.logger.errorT("Error during getMedlinePrice: " + stringWriter.toString());
			throw new ParScanResidentsEJBException(ex);
		}
		
		return currentPrice;
	}
	
	private String padLineNumber(String customer) {
		customer = padIt(customer, 10);
		return customer;
	}

	private String padIt(String result, int amount) {
		try {

			while (result.length() < amount) {
				result = "0" + result;
			}
		} catch (NumberFormatException e) {
			return result;
		}
		return result;
	}
	
	private boolean isInteger(String input)  
	{  
	   try  
	   {  
		  Integer.parseInt( input );  
		  return true;  
	   }  
	   catch(Exception e)  
	   {  
		  return false;  
	   }  
	}
	
	private static IResource getDocument(String filename)throws ResourceException, UserManagementException
	{
		IResource resource = null;
		try
		{
			IResourceContext ctx = new ResourceContext(getServiceUser());
			//RID rid = RID.getRID(IndexUtils.getPath(productNumber, extension));
			RID rid = new RID("/fileupload/InventoryMaintenance/"+filename);
			resource = ResourceFactory.getInstance().getResource(rid, ctx);
			
		}
		catch (Exception e){
			logger.exiting("ParScan getDocument Error");			
		}
		return resource;	
	}


	private static com.sapportals.portal.security.usermanagement.IUser getServiceUser() throws UserManagementException
	{
					return (com.sapportals.portal.security.usermanagement.IUser) WPUMFactory
									.getServiceUserFactory()
									.getServiceUser(
									"cmadmin_service");
	}
	
	private static String getPortalEnvironmentPrefix()
	{
		String prefix = "http://devsapep2.medline.com:50000";
		String sysname = System.getProperty("SAPSYSTEMNAME");
		if ("EPT".equals(sysname))
		{
			prefix = "http://eptest.medline.com";
		}
		else if ("EPP".equals(sysname))
		{
			prefix = "http://ep.medline.com";
		}
		else if ("EPQ".equals(sysname))
		{
			prefix = "http://staging.medline.com";
		}
	  
		return prefix;
	}
	
	private void checkIfSimilarResidentExist( ParScanResidentBean resident ) throws ParScanResidentsEJBException, SimilarResidentExistException 
	{
		String methodName = "checkIfSimilarResidentExist()";
		logger.entering( methodName );
		
		ParScanResidentBean searchCriteria = resident.getResidentExistCritera();
		String active = "X";

		List residentList = getResidentList(searchCriteria, active );
		//We are searching the resident based on id. 
		//It is observed that Z_ISA_PATIENT_SEARCH rfc in above call will return residents whose id like '%id%'
		//we need to go over the returned residents and check if the id matches with any of the resident.		
		
		if(residentList != null && !residentList.isEmpty())
		{
			Iterator iter = residentList.iterator();
			while(iter.hasNext())
			{
				ParScanResidentBean bean = (ParScanResidentBean) iter.next();
				if(searchCriteria.getId().equalsIgnoreCase(bean.getId()))
				{
					throw new SimilarResidentExistException();
				}
			}
	
		}
		
		logger.exiting( methodName );
	}
	
	
	private void closeDbResources( Connection conn, Statement stmt )
	{
		try
		{
			if ( (conn != null) && ( !conn.isClosed() ) )
			{
				conn.close();
			}
			if(stmt != null )
			{
				stmt.close();
			}
		}
		catch ( Exception e )
		{
			logger.errorT("closeDbResources(conn, stmt) failure  " + e.toString());
		}
	}
		
	private void closeDbResources( Statement stmt, ResultSet rs )
	{
		try
		{
			if(stmt != null )
			{
				stmt.close();
			}
			if(rs != null)
			{
				rs.close();
			}
		}
		catch ( Exception e )
		{
			logger.errorT("closeDbResources(stmt, rs) failure  " + e.toString());
		}
	}
	
	private void closeDbResources( Connection conn, Statement stmt, ResultSet rs)
	{
		try
		{
			closeDbResources( conn, stmt );
			
			if( rs != null )
			{
				rs.close();
			}
		}
		catch( Exception e )
		{
			logger.errorT("closeDbResources(conn, stmt, rs) failure  " + e.toString());
		}
	}
	
	private boolean isValidDate( String date)
	{
		return !StringUtils.isEmpty( date ) && !"0000-00-00".equalsIgnoreCase( date );
	}
	
	private String convertToyyyyMMdd( String dateStr, String format ) throws ParseException
	{
		SimpleDateFormat sourceFormat = new SimpleDateFormat( format );
		SimpleDateFormat destinationFormat = new SimpleDateFormat( "yyyyMMdd" );
		Date date = sourceFormat.parse( dateStr );
		
		return destinationFormat.format( date );
	}
	
	private boolean doesGtinExist(Connection con, GtinData gtin) throws SQLException
	{
		String methodName = "doesGtinExist()";
		logger.entering( methodName );
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			pstmt = con.prepareStatement( sqlProp.getString("doesGtinExist") );
			pstmt.setString(1, gtin.getItemId());
			pstmt.setString(2, gtin.getEanCategory());
			pstmt.setString(3, gtin.getUom());
			
			rs = pstmt.executeQuery();
			
			rs.next();
			
			return rs.getInt("gtinCount") > 0;
		}
		finally
		{
			closeDbResources(pstmt, rs);
		}
		
	}
	
	private void insertGtins(Connection con, List gtins) throws SQLException
	{
		String methodName = "insertGtins()";
		logger.entering( methodName );
		
		logger.debugT("Insert gtins.size() = " + gtins.size());
		
		PreparedStatement pstmt = null;
	
		try
		{
			pstmt = con.prepareStatement( sqlProp.getString("insertGtin") );
			
			for(Iterator iter = gtins.iterator(); iter.hasNext();)
			{
				GtinData gtin = (GtinData)iter.next();
				
				pstmt.setString(1, gtin.getItemId());
				pstmt.setString(2, gtin.getEanCategory());
				pstmt.setString(3, gtin.getUom());
				pstmt.setString(4, gtin.getGtin());
				
				pstmt.addBatch();
			}
		
			pstmt.executeBatch();
		
		}
		finally
		{
			if(pstmt != null)
			{
				pstmt.close();
			}
		}
	}
	
	private void updateGtins(Connection con, List gtins) throws SQLException
	{
		String methodName = "updateGtins()";
		logger.entering( methodName );
	
		logger.debugT("Update gtins.size() = " + gtins.size());
		
		PreparedStatement pstmt = null;
	
		try
		{
			pstmt = con.prepareStatement( sqlProp.getString("updateGtin") );
			
			for(Iterator iter = gtins.iterator(); iter.hasNext();)
			{
				GtinData gtin = (GtinData)iter.next();
				
				pstmt.setString(1, gtin.getGtin());
				pstmt.setString(2, gtin.getItemId());
				pstmt.setString(3, gtin.getEanCategory());
				pstmt.setString(4, gtin.getUom());
				
				pstmt.addBatch();
			}
					
			pstmt.executeBatch();
		
		}
		finally
		{
			if(pstmt != null)
			{
				pstmt.close();
			}
		}
	}
	
	private void deleteGtins(Connection con, List gtins) throws SQLException
	{
		String methodName = "deleteGtins()";
		logger.entering( methodName );
	
		logger.debugT("delete gtins.size() = " + gtins.size());
		
		PreparedStatement pstmt = null;
	
		try
		{
			pstmt = con.prepareStatement( sqlProp.getString("deleteGtin") );
			
			for(Iterator iter = gtins.iterator(); iter.hasNext();)
			{
				GtinData gtin = (GtinData)iter.next();
				
				pstmt.setString(1, gtin.getItemId());
				pstmt.setString(2, gtin.getEanCategory());
				pstmt.setString(3, gtin.getUom());
				
				pstmt.addBatch();
			}
		
			pstmt.executeBatch();
		
		}
		finally
		{
			if(pstmt != null)
			{
				pstmt.close();
			}
		}
	}	
	
	// fred 3
	
	public boolean isPRDEnvironment() throws ParScanResidentsEJBException
	{
		return ParscanEjbHelper.isPRDEnvironment();	
	}
}
